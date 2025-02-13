xref computation for Alfred; the xref.py CLI takes a collectionID at the first
argument, e.g. `./xref.py 7`.

The main.go reads from the `xref` river queue to run that CLI. This is how
Alfred runs it.

---

Configuration via environment; the most useful are, with their defaults:

    ALFRED_ES                      http://elastic:elastic@127.0.0.1:9200
    ALFRED_DB                      postgresql://aleph:aleph@127.0.0.1/aleph
    ALFRED_DB_FTM                  postgresql://aleph:aleph@127.0.0.1/aleph_ftm
    FTM_COMPARE_MODEL             ./data/model.pkl
    FTM_COMPARE_FREQUENCIES_DIR   ./data/word_frequencies

See xref.py for a full list.

Note that the river tables are expected to be on the FTM database, rather than
the regular one (as per Alfred).

Run setup.sh to download the models and stuff.

Or run it via the Dockerfile.
