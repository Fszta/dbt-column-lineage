---
hide:
  - navigation
  - toc
  - path
title: A change-impact decision engine for dbt
---

<div class="landing" markdown="0">

<section class="hero band"><div class="wrap">
  <div>
    <div class="eyebrow" style="color:var(--indigo-light)"><b>Change-impact</b> decision engine &middot; for <b>dbt</b></div>
    <h1>Know what breaks &mdash;<br>and what to <em>rebuild.</em></h1>
    <p class="sub">A change-impact <b>decision engine</b> for dbt. It categorizes <b>breaking vs cosmetic</b> changes, gates PRs on <b>your</b> policy over any dbt <code>meta</code>, and follows impact past dbt&rsquo;s edge into your <b>BI tools</b> &mdash; offline, from your artifacts, no warehouse, no dbt run. Built on column-level lineage.</p>
    <div class="cta">
      <a class="btn btn-primary" href="getting-started/quickstart/">Quick start &rarr;</a>
      <a class="btn btn-ghost" href="decision-engine/concepts/how-it-works/">How it works</a>
    </div>
    <div class="metrics">
      <div class="m"><b>1</b><span>breaking change</span></div>
      <div class="m"><b>1</b><span>exec dashboard hit</span></div>
      <div class="m"><b>BLOCK</b><span>policy verdict</span></div>
    </div>
  </div>

  <div class="frame">
    <div class="fh"><i style="background:#ff5f56"></i><i style="background:#ffbd2e"></i><i style="background:#27c93f"></i><span class="t">impact &middot; account_holder</span></div>
    <svg viewBox="0 0 500 300" role="img" aria-label="dbt column lineage: two chains — raw_accounts to stg_accounts to accounts, and raw_transactions to stg_transactions to transactions — both feed one finance exposure. The account_holder change propagates down the accounts chain and breaks the exposure.">
      <defs>
        <marker id="lar" markerWidth="7" markerHeight="7" refX="6" refY="3.5" orient="auto"><path d="M0 0L7 3.5L0 7z" fill="#5E6AD2"/></marker>
        <marker id="larg" markerWidth="7" markerHeight="7" refX="6" refY="3.5" orient="auto"><path d="M0 0L7 3.5L0 7z" fill="#556173"/></marker>
        <marker id="lara" markerWidth="7" markerHeight="7" refX="6" refY="3.5" orient="auto"><path d="M0 0L7 3.5L0 7z" fill="#f59e0b"/></marker>
      </defs>
      <!-- accounts chain (traced, indigo): raw_accounts -> stg_accounts -> accounts -> [break] exposure -->
      <path d="M112 86 L130 86" fill="none" stroke="#5E6AD2" stroke-width="2" marker-end="url(#lar)"/>
      <path d="M236 86 L254 86" fill="none" stroke="#5E6AD2" stroke-width="2" marker-end="url(#lar)"/>
      <path d="M348 86 C370 86 366 148 383 148" fill="none" stroke="#f59e0b" stroke-width="2" marker-end="url(#lara)"/>
      <!-- transactions chain (grey): raw_transactions -> stg_transactions -> transactions -> exposure -->
      <path d="M112 212 L130 212" fill="none" stroke="#556173" stroke-width="1.4" marker-end="url(#larg)"/>
      <path d="M236 212 L254 212" fill="none" stroke="#556173" stroke-width="1.4" marker-end="url(#larg)"/>
      <path d="M348 212 C370 212 366 148 383 148" fill="none" stroke="#556173" stroke-width="1.4" marker-end="url(#larg)"/>
      <g font-family="'JetBrains Mono',ui-monospace,monospace" font-size="10">
        <g><rect x="8" y="70" width="104" height="32" rx="7" fill="#171e2b" stroke="#2a3446"/><text x="20" y="90" fill="#94a1b8">raw_accounts</text></g>
        <g><rect x="132" y="70" width="104" height="32" rx="7" fill="#1c2438" stroke="#5E6AD2" stroke-width="1.4"/><text x="146" y="90" fill="#fff">stg_accounts</text></g>
        <g><rect x="256" y="70" width="92" height="32" rx="7" fill="#1c2438" stroke="#5E6AD2" stroke-width="1.4"/><text x="276" y="90" fill="#fff">accounts</text></g>
        <g><rect x="8" y="196" width="104" height="32" rx="7" fill="#171e2b" stroke="#2a3446"/><text x="14" y="216" fill="#94a1b8" font-size="9">raw_transactions</text></g>
        <g><rect x="132" y="196" width="104" height="32" rx="7" fill="#171e2b" stroke="#2a3446"/><text x="138" y="216" fill="#94a1b8" font-size="9">stg_transactions</text></g>
        <g><rect x="256" y="196" width="92" height="32" rx="7" fill="#171e2b" stroke="#2a3446"/><text x="268" y="216" fill="#94a1b8" font-size="9">transactions</text></g>
        <g><rect x="384" y="126" width="108" height="44" rx="9" fill="#1f1a10" stroke="#d97706" stroke-width="1.6"/><text x="398" y="146" fill="#f59e0b">finance_dash</text><text x="398" y="161" fill="#b98a3e" font-size="8.5">exposure</text></g>
      </g>
    </svg>
  </div>
</div></section>

<section class="problem band"><div class="wrap">
  <div class="eyebrow">The problem</div>
  <p class="pull">Change one column in a large dbt project and you&rsquo;re guessing. <b>Which models recompute? Which dashboards break? Should CI even let it merge?</b> Without column-level impact, every refactor is a risk you can&rsquo;t measure &mdash; and every CI gate is all-or-nothing.</p>
</div></section>

<section class="features band"><div class="wrap">
  <div class="eyebrow">The decision engine</div>
  <h2>From blast radius to a <em>decision</em>.</h2>
  <div class="dir">
    <div class="col">
      <div class="ic"><svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M9 12l2 2 4-4"/><path d="M21 12a9 9 0 11-18 0 9 9 0 0118 0z"/></svg></div>
      <h3>Breaking, or just cosmetic?</h3>
      <p>Diff the SQL <b>expression</b>, not the text. A provably-equivalent refactor doesn&rsquo;t block; a change that shifts meaning &mdash; or can&rsquo;t be proven safe &mdash; fails safe.</p>
      <div class="lnk"><a href="decision-engine/semantic-categorization/">categorize &rarr;</a></div>
    </div>
    <div class="col">
      <div class="ic"><svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/></svg></div>
      <h3>Your rules, your gate.</h3>
      <p>Write rules over <b>any</b> dbt <code>meta</code>. The tool ships the engine; <b>you</b> ship the policy. Block, warn, or schedule a <b>selective</b> rebuild &mdash; nothing about your taxonomy is hardcoded.</p>
      <div class="lnk"><a href="decision-engine/policy-gate/">gate &rarr;</a></div>
    </div>
    <div class="col">
      <div class="ic"><svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="4" width="18" height="12" rx="2"/><path d="M8 20h8M12 16v4"/></svg></div>
      <h3>Past dbt&rsquo;s edge.</h3>
      <p>Follow a column change into your <b>BI layer</b> &mdash; which dashboard, which field &mdash; so &ldquo;what breaks&rdquo; includes your BI, not just your models. <b>Metabase</b> is the first supported connector.</p>
      <div class="lnk"><a href="decision-engine/metabase/">cross-boundary &rarr;</a></div>
    </div>
  </div>
  <p class="pull" style="font-size:1rem;margin-top:2rem">All surfaced in an interactive <b><a href="decision-engine/explorer/">explorer</a></b> and as machine-readable JSON for your agents &mdash; on top of the column-level lineage it&rsquo;s built on. New here? Start with <a href="decision-engine/concepts/how-it-works/"><b>how it works</b></a>.</p>
</div></section>

<section class="qs band"><div class="wrap">
  <div>
    <div class="eyebrow">Quick start</div>
    <h2>One pip install. No dbt run required.</h2>
    <p class="lead">Every command reads your <span class="kbd">manifest.json</span> and <span class="kbd">catalog.json</span> &mdash; offline, zero-credential, it never touches your warehouse.</p>
  </div>
  <div class="term">
    <div class="bar"><i style="background:#ff5f56"></i><i style="background:#ffbd2e"></i><i style="background:#27c93f"></i><span class="t">bash</span></div>
<pre><span class="c"># install</span>
<span class="g">$</span> <span class="w">pip install</span> dbt-col-lineage

<span class="c"># explore lineage + impact in the browser</span>
<span class="g">$</span> <span class="w">dbt-col-lineage</span> --explore

<span class="c"># turn a PR into a decision: gate on your policy</span>
<span class="g">$</span> <span class="w">dbt-col-lineage</span> impact --base-manifest <span class="a">base/manifest.json</span> \
    --policy <span class="a">policy.yml</span> --fail-on <span class="a">policy</span></pre>
  </div>
</div></section>

<div class="foot band"><div class="wrap">
  <span class="brand">
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2L2 7l10 5 10-5-10-5z"/><path d="M2 17l10 5 10-5"/><path d="M2 12l10 5 10-5"/></svg>
    dbt-col-lineage
  </span>
  <span class="mono">MIT &middot; a decision engine for dbt teams who ship with confidence</span>
</div></div>

</div>
