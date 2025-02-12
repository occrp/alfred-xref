xref computation for Alfred; the xref.py CLI takes a collectionID at the first
argument, e.g. `./xref.py 7`.

The main.go reads from the `xref` river queue to run that CLI. This is how
Alfred runs it.

---

Configuration via environment; see xref.py for a list of values and their
defaults. Note that the river tables are expected to be on the FTM database,
rather than the regular one (as per Alfred).

Run setup.sh to download the models and stuff.
