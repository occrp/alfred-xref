#!/usr/bin/env python3

import logging, os, datetime, sys, enum, timeit, dataclasses, typing, random, warnings, argparse

import banal, followthemoney, itsdangerous, fingerprints
from followthemoney_compare.models import GLMBernoulli2EEvaluator
import servicelayer.env, servicelayer.util, servicelayer.logs
import flask, flask_sqlalchemy
from sqlalchemy.dialects.postgresql import JSONB, ARRAY

import elasticsearch.helpers
import werkzeug.local
import prometheus_client

# /home/martin/.local/lib/python3.13/site-packages/followthemoney_compare/models/model_base.py:48:
# FutureWarning: Downcasting object dtype arrays on .fillna, .ffill, .bfill is deprecated and will change in a future version.
# Call result.infer_objects(copy=False) instead. To opt-in to the future behavior, set `pd.set_option('future.no_silent_downcasting', True)`
#   return df[self.features].fillna(0).values
warnings.simplefilter(action='ignore', category=FutureWarning)

# /home/martin/code/occrp/alfred-xref/main.py:537:
# DeprecationWarning: The 'body' parameter is deprecated for the 'search' API and will be removed in a future version.
# Instead use API parameters directly. See https://github.com/elastic/elasticsearch-py/issues/1698 for more information
#   result = es.search(index=index, body=query)
warnings.simplefilter(action='ignore', category=DeprecationWarning)

log = logging.getLogger(__name__)

# settings.py
#############
ELASTICSEARCH_URL              = servicelayer.env.get("ALFRED_ES",         'http://elastic:elastic@127.0.0.1:9200')
DATABASE_URI                   = servicelayer.env.get("ALFRED_DB",         'postgresql://aleph:aleph@127.0.0.1/aleph')
FTM_STORE_URI                  = servicelayer.env.get("ALFRED_DB_FTM",     'postgresql://aleph:aleph@127.0.0.1/aleph_ftm')
XREF_MODEL                     = servicelayer.env.get("FTM_COMPARE_MODEL", './data/model.pkl')
# Env var picked up by ftm_compare:
#FTM_COMPARE_FREQUENCIES_DIR   = './data/word_frequencies'

# Probably less useful to set these, but the Aleph code refers it so copying
# here.
ELASTICSEARCH_TLS_CA_CERTS      = servicelayer.env.get("ELASTICSEARCH_TLS_CA_CERTS")
ELASTICSEARCH_TLS_VERIFY_CERTS  = servicelayer.env.to_bool("ELASTICSEARCH_TLS_VERIFY_CERTS")
ELASTICSEARCH_TLS_CLIENT_CERT   = servicelayer.env.get("ELASTICSEARCH_TLS_CLIENT_CERT")
ELASTICSEARCH_TLS_CLIENT_KEY    = servicelayer.env.get("ELASTICSEARCH_TLS_CLIENT_KEY")
ELASTICSEARCH_TIMEOUT           = servicelayer.env.to_int("ELASTICSEARCH_TIMEOUT", 60)
XREF_SCROLL                     = servicelayer.env.get("ALEPH_XREF_SCROLL", "5m")
XREF_SCROLL_SIZE                = servicelayer.env.get("ALEPH_XREF_SCROLL_SIZE", "1000")
APP_NAME                        = servicelayer.env.get("ALEPH_APP_NAME", "aleph")
INDEX_PREFIX                    = servicelayer.env.get("ALEPH_INDEX_PREFIX", APP_NAME)
INDEX_WRITE                     = servicelayer.env.get("ALEPH_INDEX_WRITE", "v1")
INDEX_READ                      = servicelayer.env.to_list("ALEPH_INDEX_READ", [INDEX_WRITE])
INDEX_DELETE_BY_QUERY_BATCHSIZE = servicelayer.env.to_int("ALEPH_INDEX_DELETE_BY_QUERY_BATCHSIZE", 100)
SECRET_KEY                      = servicelayer.env.get("ALEPH_SECRET_KEY", 'secret')

# Needed before importing ftmstore. What a design....
os.environ['FTM_STORE_URI'] = DATABASE_URI
import ftmstore

# servicelayer/settings.py
##########################
TESTING = False # Unit test context.

### aleph.core
##############
db = flask_sqlalchemy.SQLAlchemy()

_es_instance = None

def get_es():
    global _es_instance

    url = ELASTICSEARCH_URL
    timeout = ELASTICSEARCH_TIMEOUT
    con_opts = banal.clean_dict({
        "ca_certs":     ELASTICSEARCH_TLS_CA_CERTS,
        "verify_certs": ELASTICSEARCH_TLS_VERIFY_CERTS,
        "client_cert":  ELASTICSEARCH_TLS_CLIENT_CERT,
        "client_key":   ELASTICSEARCH_TLS_CLIENT_KEY,
    })
    for attempt in servicelayer.util.service_retries():
        try:
            if _es_instance is None:
                es = elasticsearch.Elasticsearch(url, timeout=timeout, **con_opts)
                es.info()
                _es_instance = es
            return _es_instance
        except elasticsearch.TransportError as exc:
            log.exception("ElasticSearch error: %s", exc.error)
            servicelayer.util.backoff(failures=attempt)
    raise RuntimeError("Could not connect to ElasticSearch")

es = werkzeug.local.LocalProxy(get_es)

# model/common.py
#################

ENTITY_ID_LEN = 128

class IdModel(object):
    id = db.Column(db.Integer(), primary_key=True)

class DatedModel(object):
    created_at = db.Column(db.DateTime, default=datetime.datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.datetime.utcnow)

    @classmethod
    def all(cls, deleted=False):
        return db.session.query(cls)

    @classmethod
    def all_ids(cls, deleted=False):
        q = db.session.query(cls.id)
        q = q.order_by(cls.id.asc())
        return q

    @classmethod
    def all_by_ids(cls, ids, deleted=False):
        return cls.all(deleted=deleted).filter(cls.id.in_(ids))

    @classmethod
    def by_id(cls, id, deleted=False):
        if id is None:
            return
        return cls.all(deleted=deleted).filter_by(id=id).first()

    def delete(self):
        # hard delete
        db.session.delete(self)

    def to_dict_dates(self):
        return {"created_at": self.created_at, "updated_at": self.updated_at}

class SoftDeleteModel(DatedModel):
    deleted_at = db.Column(db.DateTime, default=None, nullable=True)

    def delete(self, deleted_at=None):
        self.deleted_at = deleted_at or datetime.utcnow()
        db.session.add(self)

    def to_dict_dates(self):
        data = super(SoftDeleteModel, self).to_dict_dates()
        if self.deleted_at:
            data["deleted_at"] = self.deleted_at
        return data

    @classmethod
    def all(cls, deleted=False):
        q = super(SoftDeleteModel, cls).all()
        if not deleted:
            q = q.filter(cls.deleted_at == None)  # noqa
        return q

    @classmethod
    def all_ids(cls, deleted=False):
        q = super(SoftDeleteModel, cls).all_ids()
        if not deleted:
            q = q.filter(cls.deleted_at == None)  # noqa
        return q

    @classmethod
    def cleanup_deleted(cls):
        pq = db.session.query(cls)
        pq = pq.filter(cls.deleted_at != None)  # noqa
        log.info("[%s]: %d deleted objects", cls.__name__, pq.count())
        pq.delete(synchronize_session=False)

# model/entity.py
#################

LEGAL_ENTITY = "LegalEntity"

# model/role.py
class Role(db.Model, IdModel, SoftDeleteModel):
    """A user, group or other access control subject."""

    __tablename__ = "role"

    USER = "user"
    GROUP = "group"
    SYSTEM = "system"
    TYPES = [USER, GROUP, SYSTEM]

    SYSTEM_GUEST = "guest"
    SYSTEM_USER = "user"

    #: Generates URL-safe signatures for invitations.
    SIGNATURE = itsdangerous.URLSafeTimedSerializer(SECRET_KEY)

    #: Signature maximum age, defaults to 1 day
    SIGNATURE_MAX_AGE = 60 * 60 * 24

    foreign_id = db.Column(db.Unicode(2048), nullable=False, unique=True)
    name = db.Column(db.Unicode, nullable=False)
    email = db.Column(db.Unicode, nullable=True)
    type = db.Column(db.Enum(*TYPES, name="role_type"), nullable=False)
    api_key = db.Column(db.Unicode, nullable=True)
    api_key_digest = db.Column(db.Unicode, nullable=True)
    api_key_expires_at = db.Column(db.DateTime, nullable=True)
    api_key_expiration_notification_sent = db.Column(db.Integer, nullable=True)
    is_admin = db.Column(db.Boolean, nullable=False, default=False)
    is_muted = db.Column(db.Boolean, nullable=False, default=False)
    is_tester = db.Column(db.Boolean, nullable=False, default=False)
    is_blocked = db.Column(db.Boolean, nullable=False, default=False)
    password_digest = db.Column(db.Unicode, nullable=True)
    password = None
    reset_token = db.Column(db.Unicode, nullable=True)
    locale = db.Column(db.Unicode, nullable=True)
    last_login_at = db.Column(db.DateTime, nullable=True)

    permissions = db.relationship("Permission", backref="role")

# model/collection.py
#####################

class Collection(db.Model, IdModel, SoftDeleteModel):
    """A set of documents and entities against which access control is
    enforced."""

    # Category schema for collections.
    # TODO: should this be configurable?
    CATEGORIES = {
        "news": "News archives",
        "leak": "Leaks",
        "land": "Land registry",
        "gazette": "Gazettes",
        "court": "Court archives",
        "company": "Company registries",
        "sanctions": "Sanctions lists",
        "procurement": "Procurement",
        "finance": "Financial records",
        "grey": "Grey literature",
        "library": "Document libraries",
        "license": "Licenses and concessions",
        "regulatory": "Regulatory filings",
        "poi": "Persons of interest",
        "customs": "Customs declarations",
        "census": "Population census",
        "transport": "Air and maritime registers",
        "casefile": "Investigations",
        "other": "Other material",
    }
    CASEFILE = "casefile"

    # How often a collection is updated:
    FREQUENCIES = {
        "unknown": "not known",
        "never": "not updated",
        "daily": "daily",
        "weekly": "weekly",
        "monthly": "monthly",
        "annual": "annual",
    }
    DEFAULT_FREQUENCY = "unknown"

    label = db.Column(db.Unicode)
    summary = db.Column(db.Unicode, nullable=True)
    category = db.Column(db.Unicode, nullable=True)
    frequency = db.Column(db.Unicode, nullable=True)
    countries = db.Column(ARRAY(db.Unicode()), default=[])
    languages = db.Column(ARRAY(db.Unicode()), default=[])
    foreign_id = db.Column(db.Unicode, unique=True, nullable=False)
    publisher = db.Column(db.Unicode, nullable=True)
    publisher_url = db.Column(db.Unicode, nullable=True)
    info_url = db.Column(db.Unicode, nullable=True)
    data_url = db.Column(db.Unicode, nullable=True)

    # Collection inherits the `updated_at` column from `DatedModel`.
    # These two fields are used to express different semantics: while
    # `updated_at` is used to describe the last change of the metadata,
    # `data_updated_at` keeps application-level information on the last
    # change to the data within this collection.
    data_updated_at = db.Column(db.DateTime, nullable=True)

    # This collection is marked as super-secret:
    restricted = db.Column(db.Boolean, default=False)

    # Run xref on entity changes:
    xref = db.Column(db.Boolean, default=False)

    creator_id = db.Column(db.Integer, db.ForeignKey("role.id"), nullable=True)
    creator = db.relationship(Role)


# model/entityset.py
####################

class Judgement(enum.Enum):
    POSITIVE = "positive"
    NEGATIVE = "negative"
    UNSURE = "unsure"
    NO_JUDGEMENT = "no_judgement"

    def __add__(self, other):
        if self == other:
            return self
        if Judgement.NEGATIVE in (self, other):
            return Judgement.NEGATIVE
        return Judgement.UNSURE

    def to_dict(self):
        return str(self.value)

class EntitySet(db.Model, SoftDeleteModel):
    __tablename__ = "entityset"

    # set types
    LIST = "list"
    DIAGRAM = "diagram"
    TIMELINE = "timeline"
    PROFILE = "profile"

    TYPES = frozenset([LIST, DIAGRAM, TIMELINE, PROFILE])

    id = db.Column(db.String(ENTITY_ID_LEN), primary_key=True)
    label = db.Column(db.Unicode)
    type = db.Column(db.String(10), index=True, default=LIST)
    summary = db.Column(db.Unicode, nullable=True)
    layout = db.Column("layout", JSONB, nullable=True)

    role_id = db.Column(db.Integer, db.ForeignKey("role.id"), index=True)
    role = db.relationship(Role)

    collection_id = db.Column(db.Integer, db.ForeignKey("collection.id"), index=True)
    collection = db.relationship(Collection)

    parent_id = db.Column(db.String(ENTITY_ID_LEN), db.ForeignKey("entityset.id"))
    parent = db.relationship("EntitySet", backref="children", remote_side=[id])

    @classmethod
    def entity_entitysets(cls, entity_id, collection_id=None):
        """Retuns EntitySets linked positive to entity_id."""
        q = db.session.query(cls.id)
        q = q.join(EntitySetItem)
        q = q.filter(cls.deleted_at == None)  # NOQA
        q = q.filter(EntitySetItem.deleted_at == None)  # NOQA
        q = q.filter(EntitySetItem.entity_id == entity_id)
        q = q.filter(EntitySetItem.judgement == Judgement.POSITIVE)
        if collection_id:
            q = q.filter(EntitySetItem.collection_id == collection_id)
        return set([id_ for id_, in q.all()])

class EntitySetItem(db.Model, SoftDeleteModel):
    __tablename__ = "entityset_item"

    id = db.Column(db.Integer, primary_key=True)
    entityset_id = db.Column(
        db.String(ENTITY_ID_LEN), db.ForeignKey("entityset.id"), index=True
    )
    entity_id = db.Column(db.String(ENTITY_ID_LEN), index=True)
    collection_id = db.Column(db.Integer, db.ForeignKey("collection.id"), index=True)

    compared_to_entity_id = db.Column(db.String(ENTITY_ID_LEN))
    added_by_id = db.Column(db.Integer, db.ForeignKey("role.id"))
    judgement = db.Column(db.Enum(Judgement))

    entityset = db.relationship(EntitySet)
    collection = db.relationship(Collection)
    added_by = db.relationship(Role)


# model/permission.py
#####################

class Permission(db.Model, IdModel, DatedModel):
    """A set of rights granted to a role on a collection."""

    __tablename__ = "permission"

    id = db.Column(db.Integer, primary_key=True)
    role_id = db.Column(db.Integer, db.ForeignKey("role.id"), index=True)
    read = db.Column(db.Boolean, default=False)
    write = db.Column(db.Boolean, default=False)
    collection_id = db.Column(db.Integer, nullable=False)

    def to_dict(self):
        data = self.to_dict_dates()
        data.update(
            {
                "id": stringify(self.id),
                "role_id": stringify(self.role_id),
                "collection_id": stringify(self.collection_id),
                "read": self.read,
                "write": self.write,
            }
        )
        return data


# logic/xref.py
###############

ORIGIN = "xref"
MODEL = None
FTM_VERSION_STR = f"ftm-{followthemoney.__version__}"
SCORE_CUTOFF = 0.5

XREF_ENTITIES = prometheus_client.Counter(
    "aleph_xref_entities_total",
    "Total number of entities and mentions that have been xref'ed",
)

XREF_MATCHES = prometheus_client.Histogram(
    "aleph_xref_matches",
    "Number of matches per xref'ed entitiy or mention",
    # Listing 0 as a separate bucket size because it's interesting to know what
    # percentage of entities result in no matches at all
    buckets=[0, 5, 10, 25, 50],
)

XREF_CANDIDATES_QUERY_DURATION = prometheus_client.Histogram(
    "aleph_xref_candidates_query_duration_seconds",
    "Processing duration of the candidates query (excl. network, serialization etc.)",
)

XREF_CANDIDATES_QUERY_ROUNDTRIP_DURATION = prometheus_client.Histogram(
    "aleph_xref_candidates_query_roundtrip_duration_seconds",
    "Roundtrip duration of the candidates query (incl. network, serialization etc.)",
)

@dataclasses.dataclass
class Match:
    score: float
    method: str
    entity: followthemoney.proxy.EntityProxy
    collection_id: str
    match: followthemoney.proxy.EntityProxy
    entityset_ids: typing.Sequence[str]
    doubt: typing.Optional[float] = None


def xref_collection(collection_id):
    """Cross-reference all the entities and documents in a collection"""

    log.info(f"[{collection_id}] xref_collection scroll settings: scroll={XREF_SCROLL}, scroll_size={XREF_SCROLL_SIZE}")
    log.info(f"[{collection_id}] Clearing previous xref state....")

    delete_xref(collection_id, sync=True)
    delete_entities(collection_id, origin=ORIGIN, sync=True)

    index_matches(collection_id, _query_entities(collection_id))
    index_matches(collection_id, _query_mentions(collection_id))

    log.info(f"[{collection_id}] Xref done, re-indexing to reify mentions...")

    # TODO: do we really need to do this?
    #reindex_collection(collection_id, sync=False)

def _query_entities(collection_id):
    """Generate matches for indexing."""

    log.info("[%s] Generating entity-based xref...", collection_id)

    matchable = [s for s in followthemoney.model if s.matchable]
    for proxy in iter_proxies(
        collection_id  = collection_id,
        schemata       = matchable,
        es_scroll      = XREF_SCROLL,
        es_scroll_size = XREF_SCROLL_SIZE,
    ):
        yield from _query_item(proxy)

def _query_mentions(collection_id):
    aggregator = get_aggregator(collection_id, origin=ORIGIN)
    aggregator.delete(origin=ORIGIN)

    writer = aggregator.bulk()
    for proxy in _iter_mentions(collection_id):
        schemata = set()
        countries = set()
        for match in _query_item(proxy, entitysets=False):
            schemata.add(match.match.schema)
            countries.update(match.match.get_type_values(followthemoney.types.registry.country))
            match.entityset_ids = []
            yield match

        if len(schemata):
            # Assign only those countries that are backed by one of the matches:
            countries = countries.intersection(proxy.get("country"))
            proxy.set("country", countries)

            # Try to be more specific about schema:
            _merge_schemata(proxy, schemata)

            # Pick a principal name:
            proxy = name_entity(proxy)
            proxy.context["mutable"] = True
            log.debug("Reifying [%s]: %s", proxy.schema.name, proxy)
            writer.put(proxy, fragment="mention")
            # pprint(proxy.to_dict())
    writer.flush()

def _iter_mentions(collection_id):
    """Combine mentions into pseudo-entities used for xref."""

    log.info("[%s] Generating mention-based xref...", collection_id)

    proxy = followthemoney.model.make_entity(LEGAL_ENTITY)
    for mention in iter_proxies(
        collection_id  = collection_id,
        schemata       = ["Mention"],
        sort           = {"properties.resolved": "desc"},
        es_scroll      = XREF_SCROLL,
        es_scroll_size = XREF_SCROLL_SIZE,
    ):
        resolved_id = mention.first("resolved")
        if resolved_id != proxy.id:
            if proxy.id is not None:
                yield proxy
            proxy = followthemoney.model.make_entity(LEGAL_ENTITY)
            proxy.id = resolved_id

        _merge_schemata(proxy, mention.get("detectedSchema"))
        proxy.add("name", mention.get("name"))
        proxy.add("country", mention.get("contextCountry"))

    if proxy.id is not None:
        yield proxy

def _merge_schemata(proxy, schemata):
    for other in schemata:
        try:
            other = followthemoney.model.get(other)
            proxy.schema = followthemoney.model.common_schema(proxy.schema, other)
        except InvalidData:
            proxy.schema = followthemoney.model.get(LEGAL_ENTITY)

def _query_item(entity, entitysets=True):
    """Cross-reference an entity or document, given as an indexed document."""
    query = match_query(entity)
    if query == none_query():
        return

    entityset_ids = EntitySet.entity_entitysets(entity.id) if entitysets else []
    query = {"query": query, "size": 50, "_source": ENTITY_SOURCE}
    schemata = list(entity.schema.matchable_schemata)
    index = entities_read_index(schema=schemata, expand=False)

    start_time = timeit.default_timer()
    result = es.search(index=index, body=query)
    roundtrip_duration = max(0, timeit.default_timer() - start_time)
    query_duration = result.get("took")
    if query_duration is not None:
        # ES returns milliseconds, but we track query time in seconds
        query_duration = result.get("took") / 1000

    candidates = []
    for result in result.get("hits").get("hits"):
        result = unpack_result(result)
        if result is None:
            continue
        candidate = followthemoney.model.get_proxy(result)
        candidates.append(candidate)
    log.debug("Candidate [%s]: %s: %d possible matches", entity.schema.name, entity.caption, len(candidates))

    results = _bulk_compare([(entity, c) for c in candidates])
    match_count = 0
    for match, (score, doubt, method) in zip(candidates, results):
        log.debug("Match: %s: %s <[%.2f]@%0.2f> %s", method, entity.caption, score or 0.0, doubt or 0.0, match.caption)
        if score > 0:
            yield Match(
                score=score,
                doubt=doubt,
                method=method,
                entity=entity,
                collection_id=match.context.get("collection_id"),
                match=match,
                entityset_ids=entityset_ids,
            )
        if score > SCORE_CUTOFF:
            # While we store all xref matches with a score > 0, we only count matches
            # with a score above a threshold. This is in line with the user-facing behavior
            # which also only shows matches above the threshold.
            match_count += 1

    XREF_ENTITIES.inc()
    XREF_MATCHES.observe(match_count)
    XREF_CANDIDATES_QUERY_ROUNDTRIP_DURATION.observe(roundtrip_duration)
    if query_duration:
        XREF_CANDIDATES_QUERY_DURATION.observe(query_duration)

def _bulk_compare(proxies):
    global XREF_MODEL
    if not proxies:
        return
    if XREF_MODEL is None:
        yield from _bulk_compare_ftm(proxies)
        return
    global MODEL
    if MODEL is None:
        try:
            with open(XREF_MODEL, "rb") as fd:
                MODEL = GLMBernoulli2EEvaluator.from_pickles(fd.read())
        except FileNotFoundError:
            log.exception(f"Could not find model file: {XREF_MODEL}")
            XREF_MODEL = None
            yield from _bulk_compare_ftm(proxies)
            return
    yield from _bulk_compare_ftmc_model(proxies)

def _bulk_compare_ftmc_model(proxies):
    for score, confidence in zip(*MODEL.predict_proba_std(proxies)):
        yield score, confidence, MODEL.version


# index/xref.py
###############

XREF_SOURCE = {"excludes": ["text", "countries", "entityset_ids"]}
MAX_NAMES = 30

def xref_index():
    return index_name("xref", "v1")

def index_matches(collection_id, matches, sync=False):
    """Index cross-referencing matches."""
    bulk_actions(_index_form(collection_id, matches), sync=sync)

def _index_form(collection_id, matches):
    now = datetime.datetime.utcnow().isoformat()
    for match in matches:
        xref_id = banal.hash_data((match.entity.id, collection_id, match.match.id))
        text = set([match.entity.caption, match.match.caption])
        text.update(match.entity.get_type_values(followthemoney.types.registry.name)[:MAX_NAMES])
        text.update(match.match.get_type_values(followthemoney.types.registry.name)[:MAX_NAMES])
        countries = set(match.entity.get_type_values(followthemoney.types.registry.country))
        countries.update(match.match.get_type_values(followthemoney.types.registry.country))
        yield {
            "_id": xref_id,
            "_index": xref_index(),
            "_source": {
                "score": match.score,
                "doubt": match.doubt,
                "method": match.method,
                "random": random.randint(1, 2**31),
                "entity_id": match.entity.id,
                "schema": match.match.schema.name,
                "collection_id": collection_id,
                "entityset_ids": list(match.entityset_ids),
                "match_id": match.match.id,
                "match_collection_id": match.collection_id,
                "countries": list(countries),
                "text": list(text),
                "created_at": now,
            },
        }

def delete_xref(collection_id, entity_id=None, sync=False):
    """Delete xref matches of an entity or a collection."""
    shoulds = [
        {"term": {"collection_id": collection_id}},
        {"term": {"match_collection_id": collection_id}},
    ]
    if entity_id is not None:
        shoulds = [
            {"term": {"entity_id": entity_id}},
            {"term": {"match_id": entity_id}},
        ]
    query = {"bool": {"should": shoulds, "minimum_should_match": 1}}
    query_delete(xref_index(), query, sync=sync)

# index/util.py
###############

BULK_PAGE = 500
MAX_TIMEOUT = "700m"
MAX_REQUEST_TIMEOUT = 84600

def index_name(name, version):
    return "-".join((INDEX_PREFIX, name, version))

def bool_query():
    return {"bool": {"should": [], "filter": [], "must": [], "must_not": []}}

def none_query(query=None):
    if query is None:
        query = bool_query()
    query["bool"]["must"].append({"match_none": {}})
    return query

def bulk_actions(actions, chunk_size=BULK_PAGE, sync=False):
    """Bulk indexing with timeouts, bells and whistles."""

    # start_time = time()
    stream = elasticsearch.helpers.streaming_bulk(
        es,
        actions,
        chunk_size=chunk_size,
        max_retries=10,
        yield_ok=False,
        raise_on_error=False,
        refresh=refresh_sync(sync),
        request_timeout=MAX_REQUEST_TIMEOUT,
        timeout=MAX_TIMEOUT,
    )
    for _, details in stream:
        if details.get("delete", {}).get("status") == 404:
            continue
        log.warning("Bulk index error: %r", details)
    # duration = (time() - start_time)
    # log.debug("Bulk write: %.4fs", duration)

def refresh_sync(sync):
    if TESTING:
        return True
    return True if sync else False

def unpack_result(res):
    """Turn a document hit from ES into a more traditional JSON object."""
    error = res.get("error")
    if error is not None:
        raise RuntimeError("Query error: %r" % error)
    if res.get("found") is False:
        return
    data = res.get("_source", {})
    data["id"] = res.get("_id")
    data["_index"] = res.get("_index")

    _score = res.get("_score")
    if _score is not None and _score != 0.0 and "score" not in data:
        data["score"] = _score

    if "highlight" in res:
        data["highlight"] = []
        for key, value in res.get("highlight", {}).items():
            data["highlight"].extend(value)
    return data

def query_delete(index, query, sync=False, **kwargs):
    "Delete all documents matching the given query inside the index."
    for attempt in servicelayer.util.service_retries():
        try:
            es.delete_by_query(
                index=index,
                body={"query": query},
                _source=False,
                slices="auto",
                conflicts="proceed",
                wait_for_completion=sync,
                refresh=refresh_sync(sync),
                request_timeout=MAX_REQUEST_TIMEOUT,
                timeout=MAX_TIMEOUT,
                scroll_size=INDEX_DELETE_BY_QUERY_BATCHSIZE,
                **kwargs,
            )
            return
        except TransportError as exc:
            if exc.status_code in ("400", "403"):
                raise
            log.warning("Query delete failed: %s", exc)
            servicelayer.util.backoff(failures=attempt)

# index/collection.py
#####################

def delete_entities(collection_id, origin=None, schema=None, sync=False):
    """Delete entities from a collection."""
    filters = [{"term": {"collection_id": collection_id}}]
    if origin is not None:
        filters.append({"term": {"origin": origin}})
    query = {"bool": {"filter": filters}}
    query_delete(entities_read_index(schema), query, sync=sync)

# index/entities.py
###################

PROXY_INCLUDES = ["schema", "properties", "collection_id", "profile_id",
                  "role_id", "mutable", "created_at", "updated_at"]
ENTITY_SOURCE = {"includes": PROXY_INCLUDES}


def _source_spec(includes, excludes):
    includes = banal.ensure_list(includes)
    excludes = banal.ensure_list(excludes)
    return {"includes": includes, "excludes": excludes}

def _entities_query(filters, authz, collection_id, schemata):
    filters = filters or []
    if authz is not None:
        filters.append(authz_query(authz))
    if collection_id is not None:
        filters.append({"term": {"collection_id": collection_id}})
    # if banal.ensure_list(schemata):
    #     filters.append({"terms": {"schemata": banal.ensure_list(schemata)}})
    return {"bool": {"filter": filters}}


def iter_proxies(**kw):
    for data in iter_entities(**kw):
        schema = followthemoney.model.get(data.get("schema"))
        if schema is None:
            continue
        yield followthemoney.model.get_proxy(data)

def iter_entities(
    authz=None,
    collection_id=None,
    schemata=None,
    includes=PROXY_INCLUDES,
    excludes=None,
    filters=None,
    sort=None,
    es_scroll="5m",
    es_scroll_size=1000,
):
    """Scan all entities matching the given criteria."""
    query = {
        "query": _entities_query(filters, authz, collection_id, schemata),
        "_source": _source_spec(includes, excludes),
    }
    preserve_order = False
    if sort is not None:
        query["sort"] = banal.ensure_list(sort)
        preserve_order = True
    index = entities_read_index(schema=schemata)
    for res in elasticsearch.helpers.scan(
        es,
        index=index,
        query=query,
        timeout=MAX_TIMEOUT,
        request_timeout=MAX_REQUEST_TIMEOUT,
        preserve_order=preserve_order,
        scroll=es_scroll,
        size=es_scroll_size,
    ):
        entity = unpack_result(res)
        if entity is not None:
            yield entity

# index/indexes.py
##################

def schema_index(schema, version):
    """Convert a schema object to an index name."""
    if schema.abstract:
        raise InvalidData("Cannot index abstract schema: %s" % schema)
    name = "entity-%s" % schema.name.lower()
    return index_name(name, version=version)

def schema_scope(schema, expand=True):
    schemata = set()
    names = banal.ensure_list(schema) or followthemoney.model.schemata.values()
    for schema in names:
        schema = followthemoney.model.get(schema)
        if schema is not None:
            schemata.add(schema)
            if expand:
                schemata.update(schema.descendants)
    for schema in schemata:
        if not schema.abstract:
            yield schema

def entities_index_list(schema=None, expand=True):
    """Combined index to run all queries against."""
    for schema in schema_scope(schema, expand=expand):
        for version in INDEX_READ:
            yield schema_index(schema, version)

def entities_read_index(schema=None, expand=True):
    indexes = entities_index_list(schema=schema, expand=expand)
    return ",".join(indexes)

# logic/aggregator.py
#####################

def get_aggregator_name(collection_id):
    return "collection_%s" % collection_id

def get_aggregator(collection_id, origin="aleph"):
    """Connect to a followthemoney dataset."""
    dataset = get_aggregator_name(collection_id)
    return ftmstore.get_dataset(dataset, origin=origin)

# logic/matching.py
#####################

MAX_CLAUSES = 500
REQUIRED = [followthemoney.types.registry.name, followthemoney.types.registry.iban, followthemoney.types.registry.identifier]

def _make_queries(type_, value):
    if type_ == followthemoney.types.registry.name:
        yield {
            "match": {
                "fingerprints.text": {
                    "query": value,
                    "operator": "and",
                    "minimum_should_match": "60%",
                }
            }
        }
        fp = fingerprints.generate(value)
        if fp is None:
            return
        if fp.lower() != value.lower():
            yield {
                "match": {
                    "fingerprints.text": {
                        "query": fp,
                        "operator": "and",
                        "minimum_should_match": "60%",
                    }
                }
            }
    elif type_.group is not None:
        yield {"term": {type_.group: {"value": value}}}

def match_query(proxy, collection_ids=None, query=None):
    """Given a document or entity in indexed form, build a query that will find
    similar entities based on a variety of criteria."""

    if query is None:
        query = bool_query()

    # Don't match the query entity and source collection_id:
    must_not = []
    if proxy.id is not None:
        must_not.append({"ids": {"values": [proxy.id]}})
    # if source_collection_id is not None:
    #     must_not.append({'term': {'collection_id': source_collection_id}})
    if len(must_not):
        query["bool"]["must_not"].extend(must_not)

    collection_ids = banal.ensure_list(collection_ids)
    if len(collection_ids):
        query["bool"]["filter"].append({"terms": {"collection_id": collection_ids}})

    filters = set()
    for prop, value in proxy.itervalues():
        specificity = prop.specificity(value)
        if specificity > 0:
            filters.add((prop.type, value, specificity))

    filters = sorted(filters, key=lambda p: p[2], reverse=True)
    required = []
    for type_, value, _ in filters:
        if type_ in REQUIRED and len(required) <= MAX_CLAUSES:
            required.extend(_make_queries(type_, value))

    scoring = []
    for type_, value, _ in filters:
        clauses = len(required) + len(scoring)
        if type_ not in REQUIRED and clauses <= MAX_CLAUSES:
            scoring.extend(_make_queries(type_, value))

    if not len(required):
        # e.g. a document from which no features have been extracted.
        return none_query()

    # make it mandatory to have at least one match
    query["bool"]["must"].append(
        {"bool": {"should": required, "minimum_should_match": 1}}
    )
    query["bool"]["should"].extend(scoring)
    return query

def main():
    p = argparse.ArgumentParser(prog=sys.argv[0])
    p.add_argument('collection_id', type=int)
    p.add_argument('-dev',   action='store_true', help='format logs as plain text, rather than JSON')
    p.add_argument('-debug', action='store_true', help='enable debug logs')
    args = p.parse_args()

    servicelayer.settings.LOG_FORMAT = servicelayer.logs.LOG_FORMAT_TEXT if args.dev else servicelayer.logs.LOG_FORMAT_JSON
    servicelayer.logs.configure_logging(level=logging.DEBUG if args.debug else logging.INFO)

    # Don't log so much from ES lib.
    logging.getLogger('elasticsearch').setLevel(logging.WARNING)
    logging.getLogger('urllib3.connectionpool').setLevel(logging.INFO)

    app = flask.Flask("aleph")
    app.config.update({
        "SQLALCHEMY_DATABASE_URI" : DATABASE_URI,
        "FLASK_SKIP_DOTENV"       : True,
        "FLASK_DEBUG"             : args.debug,
        "BABEL_DOMAIN"            : "aleph",
        #"PROFILE"                 : SETTINGS.PROFILE,
    })
    db.init_app(app)

    with app.app_context():
        # No-op for testing if the script works at all.
        if args.collection_id == 0:
            return
        xref_collection(args.collection_id)

if __name__ == '__main__':
    main()
