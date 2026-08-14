## [0.9.0](https://github.com/Fszta/dbt-column-lineage/compare/v0.8.0...v0.9.0) (2026-08-14)


### Features

* **cli:** add machine-readable --format json for AI-first consumption ([9211c1f](https://github.com/Fszta/dbt-column-lineage/commit/9211c1facbbf49bf1e3fba06c05dc3b53f1e01b8))
* **impact:** add diff-driven impact command ([b7eb165](https://github.com/Fszta/dbt-column-lineage/commit/b7eb165ca824527e4c4822efbb454bdb4c776314))
* **impact:** CI mode with sticky PR comment and severity gate ([dd787af](https://github.com/Fszta/dbt-column-lineage/commit/dd787af998388fba0d7743147a86c3bb601a64c8))
* **ui:** set initial state when no model is selected ([fc92a03](https://github.com/Fszta/dbt-column-lineage/commit/fc92a03088b2b765d7199824266d132ce19f5c82))


### Bug Fixes

* **catalog:** read schema/database/name from node metadata ([c2c9053](https://github.com/Fszta/dbt-column-lineage/commit/c2c9053476119f17e0816ee6c1a381a18b4f68ea))
* **display:** handle exposures/sources sets in downstream output ([c2b8712](https://github.com/Fszta/dbt-column-lineage/commit/c2b8712c7d73cde856ca5dd909b8c6ebef8bb9da))
* **manifest:** fall back to on-disk compiled SQL when manifest lacks compiled_code ([1a1af9a](https://github.com/Fszta/dbt-column-lineage/commit/1a1af9a3fed38bcfc28301e41de3c611860d4be8))
* **parser:** resolve transitive CTE aliases to the base model ([0be42ea](https://github.com/Fszta/dbt-column-lineage/commit/0be42ea158a782c4c1e64bbe71df6104143a59b1))

## [0.8.0](https://github.com/Fszta/dbt-column-lineage/compare/v0.7.0...v0.8.0) (2025-11-30)


### Features

* add model details card component ([cf5aa7b](https://github.com/Fszta/dbt-column-lineage/commit/cf5aa7b49dfa62719fbf1e0742eca2d9a99b923c))
* add model details endpoint with desc and tags ([4cdac70](https://github.com/Fszta/dbt-column-lineage/commit/4cdac70d30b0cd762f5239d132ce9818f3bb11e8))
* **lineage:** add expand upstream lineage capability ([ef89f35](https://github.com/Fszta/dbt-column-lineage/commit/ef89f352567379dbf93aa0f2d01a083881dfbaca))
* make col relationship summary card collapsible ([21a6135](https://github.com/Fszta/dbt-column-lineage/commit/21a613574f7fb299e5e7cd91bf280d28e442c146))
* **ui:** add glassmorphism effect and polish interactions ([017b253](https://github.com/Fszta/dbt-column-lineage/commit/017b253f105b09d98fda183908161bdad0f1aa1e))
* **ui:** enhance explorer tree with badges, node count and search highlight ([f73d88b](https://github.com/Fszta/dbt-column-lineage/commit/f73d88b3303c2970ffe9a4a6ac684ba486dd3ce7))
* **ui:** improve graph viz with better shaddow and hover states ([636df01](https://github.com/Fszta/dbt-column-lineage/commit/636df0135c7e2e003711e7ff7cf084a4e1f7c67b))


### Bug Fixes

* **lineage:** align exposure edges horizontally with model edges ([38fed02](https://github.com/Fszta/dbt-column-lineage/commit/38fed021bb899497703978ff7361ab0166685a00))
* return full upstream lineage chain in api, not only n-1 ([0ece04a](https://github.com/Fszta/dbt-column-lineage/commit/0ece04a88902ee3c9eb41a04ac1f9767cf4eb2e4))

## [0.7.0](https://github.com/Fszta/dbt-column-lineage/compare/v0.6.0...v0.7.0) (2025-11-28)


### Features

* **folder-tree:** add tooltip for truncated models ([8043352](https://github.com/Fszta/dbt-column-lineage/commit/80433521975d9f509e28abb09ed3947ed76dcbe1))
* **impact-analysis:** group transformations by model with toggle ([60e5044](https://github.com/Fszta/dbt-column-lineage/commit/60e5044bd55bb70fa8fcee042b13c812335fad9f))
* **lineage:** add tooltips for truncated column and model names ([fdf141e](https://github.com/Fszta/dbt-column-lineage/commit/fdf141e65facc845e2dbc70216b16fd808182281))
* **snapshots:** add dbt snapshots support ([a84bf63](https://github.com/Fszta/dbt-column-lineage/commit/a84bf6314e07b29547e1c830aa17185f4492d16d))


### Bug Fixes

* **impact-analysis:** close buttons for impact/relationship summary cards ([6b9ddf1](https://github.com/Fszta/dbt-column-lineage/commit/6b9ddf127677037d8aecb949b700131027236dce))
* **impact-analysis:** truncate long names and fix badge pos ([f9e77b2](https://github.com/Fszta/dbt-column-lineage/commit/f9e77b283fa14294f9e6d913bcc57121216a2fab))

## [0.6.0](https://github.com/Fszta/dbt-column-lineage/compare/v0.5.0...v0.6.0) (2025-11-26)


### Features

* add column relationship summary card to lineage view ([e3b1ce9](https://github.com/Fszta/dbt-column-lineage/commit/e3b1ce97cb3046d3176b5a29a40102621f3fc286))
* add expand/collapse functionality for lineage graph ([#25](https://github.com/Fszta/dbt-column-lineage/issues/25)) ([f9a88a6](https://github.com/Fszta/dbt-column-lineage/commit/f9a88a6f0f3e299f11c498b2aad56a533408d4fd))
* add search input and filtering to column selector ([ab6c8c2](https://github.com/Fszta/dbt-column-lineage/commit/ab6c8c294612132f8f4ad9528c0efa710fabd170))
* add strip_sql_comments utility function ([28cdf95](https://github.com/Fszta/dbt-column-lineage/commit/28cdf95dffa5090d63dcafcd7bf6f144232dbec6))
* improve SQL parser with forward references, EXCLUDE clause, and nested subquery support ([#24](https://github.com/Fszta/dbt-column-lineage/issues/24)) ([4a2bc73](https://github.com/Fszta/dbt-column-lineage/commit/4a2bc739c64596bf201d7a94b3b0cd4e94013a1e))


### Bug Fixes

* handle uppercase source ([85abcee](https://github.com/Fszta/dbt-column-lineage/commit/85abcee2f9ee9afb5469a057cb760aad596aa033))
* non deterministic behavior ([7b27132](https://github.com/Fszta/dbt-column-lineage/commit/7b271326f40bbb758a087a75cde9ca8201453e26))
* strip SQL comments in parser ([db91629](https://github.com/Fszta/dbt-column-lineage/commit/db9162911f943e57776511fc58d8a5cb340d9025))
* strip SQL comments in service layer ([4f617c4](https://github.com/Fszta/dbt-column-lineage/commit/4f617c40e35c06ef7f4791390b26e0d799a1f863))

## [0.5.0](https://github.com/Fszta/dbt-column-lineage/compare/v0.4.0...v0.5.0) (2025-11-16)


### Features

* add exposures to lineage ([#18](https://github.com/Fszta/dbt-column-lineage/issues/18)) ([a03f26c](https://github.com/Fszta/dbt-column-lineage/commit/a03f26ccb5e3f41f60d303413ba09f699c9bbe49))
* add impact analysis to understand downstream effects of col changes ([#21](https://github.com/Fszta/dbt-column-lineage/issues/21)) ([1352fb4](https://github.com/Fszta/dbt-column-lineage/commit/1352fb4e7317d5322a779a9955bfd1b6100de11e))
* enhance explore panel ui ([#19](https://github.com/Fszta/dbt-column-lineage/issues/19)) ([0a0e5d6](https://github.com/Fszta/dbt-column-lineage/commit/0a0e5d6a82a8f90e0688021494429565272131c1))


### Bug Fixes

* position nodes based on data flow, not selected node ([#15](https://github.com/Fszta/dbt-column-lineage/issues/15)) ([66805ec](https://github.com/Fszta/dbt-column-lineage/commit/66805ecdacc2d7280d70341e3be7e127129ff1e5))

## [0.4.0](https://github.com/Fszta/dbt-column-lineage/compare/v0.3.1...v0.4.0) (2025-11-13)


### Features

* add explorable dbt project folder tree ([#11](https://github.com/Fszta/dbt-column-lineage/issues/11)) ([689902c](https://github.com/Fszta/dbt-column-lineage/commit/689902cf46177ce808390be61a81f88aaf5d8dab))
* add mssqlserver / tsql adapter support ([#13](https://github.com/Fszta/dbt-column-lineage/issues/13)) ([30632fd](https://github.com/Fszta/dbt-column-lineage/commit/30632fd9899f421b98cfd955aab8f41208627ef1))

## [0.3.1](https://github.com/Fszta/dbt-column-lineage/compare/v0.3.0...v0.3.1) (2025-04-12)


### Bug Fixes

* **lineage:** overlapping model boxes ([#6](https://github.com/Fszta/dbt-column-lineage/issues/6)) ([b582bea](https://github.com/Fszta/dbt-column-lineage/commit/b582bea14077e0122e6d9e090cf1d16d0711b202))

## [0.3.0](https://github.com/Fszta/dbt-column-lineage/compare/v0.2.1...v0.3.0) (2025-04-09)


### Features

* add dialect support for SQL parser ([#5](https://github.com/Fszta/dbt-column-lineage/issues/5)) ([a1060e7](https://github.com/Fszta/dbt-column-lineage/commit/a1060e7ef9f5142ed2ea2912e7075c6da4a3887c))

## [0.2.1](https://github.com/Fszta/dbt-column-lineage/compare/v0.2.0...v0.2.1) (2025-04-08)


### Bug Fixes

* model box drag behavior ([#3](https://github.com/Fszta/dbt-column-lineage/issues/3)) ([bcab221](https://github.com/Fszta/dbt-column-lineage/commit/bcab221dabcda80738aa13f5f9a5145ae4f4bc13))

## [0.2.0](https://github.com/Fszta/dbt-column-lineage/compare/v0.1.1...v0.2.0) (2025-04-08)


### Features

* add explore CLI command for interactive lineage visualization ([#1](https://github.com/Fszta/dbt-column-lineage/issues/1)) ([f804bbc](https://github.com/Fszta/dbt-column-lineage/commit/f804bbc19ad2dfdc90d63cdaed88802646745d00))

## [0.1.1](https://github.com/Fszta/dbt-column-lineage/compare/20ee116563dd2eff3233abb279531105168e5c2a...v0.1.1) (2025-03-30)


### Features

* add cli with display format ([7243593](https://github.com/Fszta/dbt-column-lineage/commit/72435938c430343ee22987437db71e9b063f5a78))
* add html lineage ([dec8602](https://github.com/Fszta/dbt-column-lineage/commit/dec86028fd90eef934046c1b0bf54ae2ddd2a92f))
* handle select star ([023a4e4](https://github.com/Fszta/dbt-column-lineage/commit/023a4e46d554077934943146069a62f7fd49bd71))
* handle star refs ([a5795de](https://github.com/Fszta/dbt-column-lineage/commit/a5795de5f91d2ed2896ca3e8ccfbfaaa9b18efd9))
* improve html display ([006a560](https://github.com/Fszta/dbt-column-lineage/commit/006a560b354a632597bbfb0668f91e00a2d689db))
* support html render ([d6a067d](https://github.com/Fszta/dbt-column-lineage/commit/d6a067db7c6cb877c32203e64d291bdd682c3f22))


### Bug Fixes

* raw table name ([20ee116](https://github.com/Fszta/dbt-column-lineage/commit/20ee116563dd2eff3233abb279531105168e5c2a))
* test csv file name ([60b8853](https://github.com/Fszta/dbt-column-lineage/commit/60b885307ae823c716287194184ef4e5f33c4ef7))
* use source name instead of identifier ([2e83693](https://github.com/Fszta/dbt-column-lineage/commit/2e8369398db0b4ec40b48eb34d19e1f7d6e5dc43))

