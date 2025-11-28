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

