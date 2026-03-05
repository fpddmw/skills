# GDELT Full Validation Record (2026-03-03)

This document keeps key validation outputs verbatim for manual inspection.

## 0) Clear Database (Verbatim)

```text
DB_CLEARED path=/home/fpddmw/projects/skills/data/gdelt_environment.db
total 60
-rw-r--r-- 1 fpddmw fpddmw 61440 Mar  3 18:42 gdelt_validation.db
```

## 1) init-db (Verbatim)

```text
$ python3 listener-gdelt-doc-ingestor/scripts/gdelt_fetch.py init-db --db /home/fpddmw/projects/skills/data/gdelt_environment.db
GDELT_DB_OK path=/home/fpddmw/projects/skills/data/gdelt_environment.db table=gdelt_environment_events
EXIT_CODE=0
```

## 2) sync --classify-mode llm (Verbatim)

```text
$ python3 listener-gdelt-doc-ingestor/scripts/gdelt_fetch.py sync --db /home/fpddmw/projects/skills/data/gdelt_environment.db --query climate --start-datetime 20260302000000 --end-datetime 20260303235959 --max-records 10 --timeout 60 --classify-mode llm --llm-timeout 45
GDELT_SYNC_OK query='climate' start=20260302000000 end=20260303235959 fetched=10 upserted=10 classify_mode=llm rows=10 unique_urls=10
EXIT_CODE=0
```

## 3) enrich --classify-mode llm (Verbatim)

```text
$ python3 listener-gdelt-doc-ingestor/scripts/gdelt_fetch.py enrich --db /home/fpddmw/projects/skills/data/gdelt_environment.db --classify-mode llm --limit 200 --llm-timeout 45
GDELT_ENRICH_OK processed=10 updated=10 classify_mode=llm
EXIT_CODE=0
```

## 4) summarize (Verbatim)

```text
$ python3 listener-gdelt-doc-ingestor/scripts/gdelt_fetch.py summarize --db /home/fpddmw/projects/skills/data/gdelt_environment.db --only-relevant --limit 500
SOCIAL_SUMMARIZE_OK source_rows=10 processed=9 upserted=9 social_rows=9
EXIT_CODE=0
```

## 5) list-events (Verbatim)

```text
$ python3 listener-gdelt-doc-ingestor/scripts/gdelt_fetch.py list-events --db /home/fpddmw/projects/skills/data/gdelt_environment.db --limit 20
id	seendate_utc	env_relevance	avg_tone	goldstein_scale	source_country	classifier	title	url
2	20260303T080000Z	0			Nepal	llm:gpt-5	आगामी सरकारको दायित्व , हिमाली मुद्दा	https://www.annapurnapost.com/story/495052/
1	20260303T073000Z	1			Sweden	llm:gpt-5	Klimatministern sågar KD : Noll trovärdighet	https://www.aftonbladet.se/nyheter/a/bOgw1A/klimatministern-sagar-kd-noll-trovardighet
7	20260303T053000Z	1			Taiwan	llm:gpt-5	落實氣候行動 、 倡議永續發展 ！ 竹市首場氣候變遷市民學堂舊港登場	https://www.cna.com.tw:443/postwrite/chi/427047
8	20260303T031500Z	1			United States	llm:gpt-5	Climate models and extreme weather : Why predictions often fail to match reality – NaturalNews . com	https://www.naturalnews.com/2026-03-02-climate-models-predictions-fail-to-match-reality.html
5	20260303T010000Z	1			Turkey	llm:gpt-5	Türkiye acts to align universities with COP31 agenda	https://www.hurriyetdailynews.com/turkiye-acts-to-align-universities-with-cop31-agenda-219513
3	20260302T180000Z	1			Nigeria	llm:gpt-5	Okereke : Why We Initiated Climate Governance Ranking for 36 States – THISDAYLIVE	https://www.thisdaylive.com/2026/03/02/okereke-why-we-initiated-climate-governance-ranking-for-36-states/
4	20260302T154500Z	1			India	llm:gpt-5	From Campus To Climate Frontlines : 24 Youth - led Social Action Projects From HSNC University That Are Reimagining Mumbai Climate Story	https://www.freepressjournal.in/education/from-campus-to-climate-frontlines-24-youth-led-social-action-projects-from-hsnc-university-that-are-reimagining-mumbais-climate-story
9	20260302T140000Z	1			United States	llm:gpt-5	Calling All Educators to Become Climate Box Educators	https://www2.fundsforngos.org/education/calling-all-educators-to-become-climate-box-educators/
6	20260302T140000Z	1			United States	llm:gpt-5	Open Call : Expanding Residential Resilience Financing for Municipalities ( Canada )	https://www2.fundsforngos.org/environment/open-call-expanding-residential-resilience-financing-for-municipalities-canada/
10	20260302T113000Z	1			Netherlands	llm:gpt-5	Subnational Climate Leadership Dynamics Among Under2 Coalition Members	https://link.springer.com/book/10.1007/978-3-032-12610-8
EXIT_CODE=0
```

## 6) DB Counts (Verbatim)

```text
$ python3 (db counts) target=/home/fpddmw/projects/skills/data/gdelt_environment.db
raw_rows=10
relevant_rows=9
social_rows=9
llm_rows=10
top_social_rows:
(9, 'https://link.springer.com/book/10.1007/978-3-032-12610-8', 1, 'llm:gpt-5', '20260302T113000Z')
(8, 'https://www2.fundsforngos.org/environment/open-call-expanding-residential-resilience-financing-for-municipalities-canada/', 1, 'llm:gpt-5', '20260302T140000Z')
(7, 'https://www2.fundsforngos.org/education/calling-all-educators-to-become-climate-box-educators/', 1, 'llm:gpt-5', '20260302T140000Z')
(6, 'https://www.freepressjournal.in/education/from-campus-to-climate-frontlines-24-youth-led-social-action-projects-from-hsnc-university-that-are-reimagining-mumbais-climate-story', 1, 'llm:gpt-5', '20260302T154500Z')
(5, 'https://www.thisdaylive.com/2026/03/02/okereke-why-we-initiated-climate-governance-ranking-for-36-states/', 1, 'llm:gpt-5', '20260302T180000Z')
(4, 'https://www.hurriyetdailynews.com/turkiye-acts-to-align-universities-with-cop31-agenda-219513', 1, 'llm:gpt-5', '20260303T010000Z')
(3, 'https://www.naturalnews.com/2026-03-02-climate-models-predictions-fail-to-match-reality.html', 1, 'llm:gpt-5', '20260303T031500Z')
(2, 'https://www.cna.com.tw:443/postwrite/chi/427047', 1, 'llm:gpt-5', '20260303T053000Z')
(1, 'https://www.aftonbladet.se/nyheter/a/bOgw1A/klimatministern-sagar-kd-noll-trovardighet', 1, 'llm:gpt-5', '20260303T073000Z')
EXIT_CODE=0
```

## 7) quick_validate (Verbatim)

```text
Skill is valid!
```
