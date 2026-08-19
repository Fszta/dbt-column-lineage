---
hide:
  - navigation
  - toc
  - path
title: Impact analysis for dbt, column by column
---

<div class="landing" markdown="0">

<section class="hero band"><div class="wrap">
  <div>
    <div class="eyebrow" style="color:var(--indigo-light)"><b>Column-level</b> lineage &middot; <b>impact</b> analysis</div>
    <h1>Know what breaks<br>before you <em>change it.</em></h1>
    <p class="sub"><b>Column-level</b> data lineage for dbt projects. Trace any <b>column</b> upstream and down, and see a change's exact <b>impact</b> before you ship it.</p>
    <div class="cta">
      <a class="btn btn-primary" href="getting-started/quickstart/">Quick start &rarr;</a>
      <a class="btn btn-ghost" href="https://dbt-column-lineage.onrender.com">Explore a live project</a>
    </div>
    <div class="metrics">
      <div class="m"><b>6</b><span>columns changed</span></div>
      <div class="m"><b>1</b><span>mart downstream</span></div>
      <div class="m"><b>1</b><span>exposure hit</span></div>
    </div>
  </div>

  <div class="frame">
    <div class="fh"><i style="background:#ff5f56"></i><i style="background:#ffbd2e"></i><i style="background:#27c93f"></i><span class="t">lineage &middot; account_holder</span></div>
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
  <p class="pull">Change one column in a large dbt project and you&rsquo;re guessing. <b>Which models depend on it? Which dashboards break?</b> Without column-level visibility, every refactor is a risk you can&rsquo;t measure.</p>
</div></section>

<section class="features band"><div class="wrap">
  <div class="eyebrow">Why it matters</div>
  <h2>See the blast radius, then ship with confidence.</h2>
  <div class="dir">
    <div class="col">
      <div class="ic"><svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M9 12l2 2 4-4"/><path d="M21 12a9 9 0 11-18 0 9 9 0 0118 0z"/></svg></div>
      <h3>Safe refactoring</h3>
      <p>Change a column knowing exactly what it touches, upstream and down &mdash; no guesswork.</p>
      <div class="lnk"><a href="features/impact-analysis/">impact &rarr;</a></div>
    </div>
    <div class="col">
      <div class="ic"><svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M13 2L3 14h9l-1 8 10-12h-9z"/></svg></div>
      <h3>Faster development</h3>
      <p>Trace dependencies in seconds instead of grepping models. Understand the graph instantly.</p>
      <div class="lnk"><a href="getting-started/quickstart/">explore &rarr;</a></div>
    </div>
    <div class="col">
      <div class="ic"><svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M3 3v18h18"/><path d="M7 14l4-4 3 3 5-6"/></svg></div>
      <h3>CI impact gating</h3>
      <p>Fail a PR when a change hits a critical exposure. Machine-readable output feeds your agents.</p>
      <div class="lnk"><a href="features/impact-analysis/">ci &rarr;</a></div>
    </div>
  </div>
</div></section>

<section class="qs band"><div class="wrap">
  <div>
    <div class="eyebrow">Quick start</div>
    <h2>One pip install. No dbt run required.</h2>
    <p class="lead">It reads your <span class="kbd">manifest.json</span> and <span class="kbd">catalog.json</span> &mdash; it never runs your warehouse.</p>
  </div>
  <div class="term">
    <div class="bar"><i style="background:#ff5f56"></i><i style="background:#ffbd2e"></i><i style="background:#27c93f"></i><span class="t">bash</span></div>
<pre><span class="c"># install</span>
<span class="g">$</span> <span class="w">pip install</span> dbt-col-lineage

<span class="c"># explore column lineage in the browser</span>
<span class="g">$</span> <span class="w">dbt-col-lineage</span> --explore

<span class="c"># what breaks if orders.amount changes?</span>
<span class="g">$</span> <span class="w">dbt-col-lineage</span> --select <span class="a">orders.amount+</span> --format json</pre>
  </div>
</div></section>

<div class="foot band"><div class="wrap">
  <span class="brand">
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2L2 7l10 5 10-5-10-5z"/><path d="M2 17l10 5 10-5"/><path d="M2 12l10 5 10-5"/></svg>
    dbt-col-lineage
  </span>
  <span class="mono">MIT &middot; built for dbt teams who refactor with confidence</span>
</div></div>

</div>
