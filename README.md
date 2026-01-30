
---

# MetisMedia

**MetisMedia is an AI-native platform for building high-impact media lists and outreach strategies—faster, safer, and with far more precision than legacy tools.**

We help communications strategists, advocacy teams, and modern PR firms answer questions like:

* *Which Substack writers are shaping how crypto developers think right now?*
* *Who actually influences the conversation around climate policy in Arizona this month?*
* *Which voices matter for this campaign—not in theory, but in practice?*

And we do it in **minutes instead of days**.

---

## What Is a Media List?

A **media list** is the collection of journalists, writers, and commentators that a communications strategist targets to shape and influence the narrative around a given campaign.

Historically, media lists were built using static databases like **Cision** or **MuckRack**. These tools focus on legacy outlets with the assumption influence flows through institutional media.

That assumption is now broken.

Influence has shifted to:

* Independent **Substack writers**
* Highly-focused **niche podcasters**
* Subject-matter experts on emerging platforms
* Creators whose audiences are smaller—but *deeply aligned and highly influential*

Because legacy tools haven’t evolved, strategists are forced to manually hunt for these voices—googling, reading threads, skimming newsletters, and stitching together context by hand. Building a single high-quality list can take days.

---

## What MetisMedia Does Differently

MetisMedia doesn’t just improve media lists.
It **changes how they’re built**.

Instead of starting with a database of people, MetisMedia starts with the **campaign itself**.

Our AI system, **Metis**, first understands:

* the campaign’s intent and stakes
* the audience that matters
* tone and positioning constraints
* geography, platform, and risk considerations

Only then does she reason about **which voices actually shape that audience’s thinking today**.

From there, Metis:

1. **Actively identifies relevant voices** across modern platforms
2. **Reads and analyzes recent content** to understand each person’s perspective
3. **Returns evidence-backed summaries** explaining *why* each person belongs on the list
4. **Drafts hyper-personalized outreach** grounded in that context

The result isn’t just better targeting—it’s a **massive return of time**.
What once took days of manual research, vetting, and drafting now takes **20–30 minutes**.

---

## The Core Insight

Modern communications isn’t a database problem.
It’s a **workflow and reasoning problem**.

The hard part isn’t *finding* names.
It’s:

* knowing who actually matters *right now*
* understanding their stance and recent thinking
* avoiding reputational or outreach mistakes
* and doing all of that fast, under pressure

MetisMedia is built to solve that entire workflow—not just one step.

---

## How the System Works (Conceptual)

MetisMedia is an **event-driven, agentic system** composed of specialized nodes. Each node has a narrow responsibility, and together they form a safe, auditable pipeline from campaign intent to finished outreach.

You don’t need to understand the internals to use the product—but this architecture is what makes it defensible.

### Node A — Campaign Briefing

Metis conducts a structured briefing with the strategist—via text or voice—to understand:

* intent
* audience
* tone
* constraints
* risk factors

This ensures the system reasons from *strategy*, not keywords.

### Node B — Safety & Relevance Gate

Before any outreach or discovery happens, the system enforces:

* relevance thresholds
* freshness checks
* outreach safety rules
* budget constraints

This prevents wasted effort, over-outreach, and reputational mistakes.

### Node C — Discovery & Evidence

Metis identifies relevant voices and analyzes their **recent content**, producing:

* clear summaries of stance and perspective
* direct “receipts” (quotes, excerpts, timestamps)
* transparent reasoning for inclusion

### Node D/E — Profiling & Contact Readiness

The system prepares each target for outreach by understanding:

* how they prefer to be contacted
* whether outreach is appropriate
* what context should be referenced

### Node F — Personalized Drafting

Using all prior context, Metis drafts tailored pitch emails or messages that feel human, informed, and specific.

### Node G — Finalization & Learning

The system assembles the final dossier and tracks outcomes—feeding learnings back into future runs.

---

## Why This Is Hard to Copy

MetisMedia isn’t a wrapper around an LLM.

It’s a **production-grade orchestration system** with:

* strict multi-tenant isolation
* deterministic idempotency (no double actions)
* budget and cost enforcement
* retry and failure handling
* full auditability of decisions

This allows us to operate in high-stakes environments—advocacy, policy, crisis communications—where mistakes are expensive.

---

## Current State

MetisMedia is already running end-to-end in a local environment:

* Campaign briefing API (Node A)
* Event-driven orchestration across all nodes
* Budget enforcement and cost accounting
* Evidence-backed target generation (mocked for now)
* Fully automated dossier creation

External data providers (search, scraping, embeddings) are intentionally gated and will be integrated incrementally.

---

## What’s Next

* Integrate real embedding and discovery providers (starting conservatively)
* Harden Node B/C logic with live data
* Build a strategist-facing dashboard
* Launch pilot programs with select comms and advocacy teams

---

## Why Now

The communications landscape has already changed.
The tools haven’t.

MetisMedia is built for how influence actually works today—and for the teams who don’t have time to pretend otherwise.
