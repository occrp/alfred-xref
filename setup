#!/bin/sh
set -euC

#dst='/opt/ftm-compare'
dst='./data'

mkdir -p "$dst/word_frequencies"

curl -Lo "$dst/word_frequencies.zip" \
  'https://public.data.occrp.org/develop/models/word_frequencies/word_frequencies-v0.4.1.zip'
curl -Lo "$dst/model.pkl" \
	'https://public.data.occrp.org/develop/models/xref/glm_bernoulli_2e_wf-v0.4.1.pkl'

python3 -m zipfile --extract \
	"$dst/word_frequencies.zip" \
	"$dst/word_frequencies"
