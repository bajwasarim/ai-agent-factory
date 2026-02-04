# Milestone-Based Roadmap Timeline (Feature Gates)

This roadmap defines execution phases as **feature-gated milestones**. Progression to the next phase requires explicit technical acceptance criteria to be met.

---

## Phase 0 — Core Ingestion Foundation ✅ (COMPLETED)

### Objective
Establish a reliable data ingestion and normalization backbone.

### Delivered Capabilities
- Google Maps ingestion agent
- Business normalization and schema standardization
- Idempotent deduplication logic
- Segmentation + geo-radius query support
- Stable pipeline orchestration

### Exit Criteria (Gate)
- Maps ingestion stable with multiple queries
- Idempotent export verified
- Normalized business schema contract locked

---

## Phase 1 — Validation + Routing Layer ✅ (COMPLETED)

### Objective
Classify leads by website presence and route them deterministically.

### Delivered Capabilities
- WebsitePresenceValidatorAgent
- LeadRouterAgent with route logic
- Routing categories: TARGET / EXCLUDED / RETRY
- Deterministic ordering and stats output

### Exit Criteria (Gate)
- Website validation rules enforced
- Routing logic fully tested
- Output contracts verified

---

## Phase 2 — Contract Normalization + Fan-Out Export ✅ (COMPLETED)

### Objective
Harden pipeline contracts and enable multi-sheet export fan-out.

### Delivered Capabilities
- Pipeline contract normalization
- Dedup ownership lock
- Fan-out export architecture
- Atomic preflight/write/backup flow
- Per-sheet idempotency

### Exit Criteria (Gate)
- Export fan-out tested
- Atomic export guarantees verified
- Contract tests passing

---

## Phase 3 — Retry Pipeline Architecture ✅ (COMPLETED)

### Objective
Enable safe reprocessing of failed website validations.

### Delivered Capabilities
- RetryInputLoaderAgent
- Retry counter logic
- ENV-configurable retry limits
- Dual-mode pipeline (normal / retry)
- CLI execution modes

### Exit Criteria (Gate)
- Retry-only pipeline operational
- Max retry enforcement tested
- Retry stats validated

---

## Phase 4 — Lead Qualification & Enrichment (NEXT)

### Objective
Enhance lead quality before outreach and landing generation.

### Planned Capabilities
- Business category scoring
- Contact completeness scoring
- Business size estimation heuristics
- Location confidence scoring

### Exit Criteria (Gate)
- Lead scoring thresholds defined
- Enrichment pipeline integrated
- Scored leads exported

---

## Phase 5 — Landing Page Automation System

### Objective
Automatically generate conversion-focused landing pages for TARGET leads.

### Planned Capabilities
- Template-based landing generator
- Dynamic content injection
- SEO meta generation
- Business branding customization

### Exit Criteria (Gate)
- Static site generation working
- One-click deploy ready
- Preview pipeline operational

---

## Phase 6 — Outreach Automation Layer

### Objective
Automate outreach to generated leads.

### Planned Capabilities
- Email campaign generator
- WhatsApp outreach integration
- CRM export adapters
- Reply tracking hooks

### Exit Criteria (Gate)
- Outreach templates validated
- Campaign automation working
- Engagement metrics captured

---

## Phase 7 — Monetization & Scaling Infrastructure

### Objective
Prepare platform for commercial scale.

### Planned Capabilities
- Multi-user account isolation
- Usage quota enforcement
- Billing integration
- Performance monitoring

### Exit Criteria (Gate)
- Multi-tenant support
- Billing flows validated
- Observability dashboards online

---

## Execution Philosophy

- No phase advances without automated test coverage
- Contract stability is enforced before feature expansion
- Infrastructure reliability precedes automation features
- Revenue features only built after data quality stabilization

---

Status Summary

Phase 0–3: COMPLETE
Phase 4: READY TO START
Phase 5–7: PLANNED


---

## Roadmap ↔ Architecture Phase Alignment (Cross-Reference)

| Milestone ID | Roadmap Feature Gate | Architecture Spec Phase Reference | Description |
|-------------|----------------------|-----------------------------------|-------------|
| M1 | Core Ingestion Pipeline Stable | Phase 2.1 — Data Acquisition Layer | Maps ingestion, normalization, dedup foundation |
| M2 | Website Validation + Routing | Phase 2.2 — Validation & Qualification Layer | WebsitePresenceValidator, LeadRouterAgent integration |
| M3 | Fan-Out Export + Atomic Writes | Phase 2.3 — Export & Persistence Layer | Multi-sheet fan-out, idempotent export, backups |
| M4 | Retry Pipeline Activation | Phase 2.4 — Recovery & Retry Subsystem | RetryInputLoaderAgent, retry mode pipeline |
| M5 | Outreach Data Readiness | Phase 3.1 — CRM/Outbound Integration Layer | Clean lead export contract for downstream outreach |
| M6 | Landing Page Generator MVP | Phase 3.2 — Automated Web Asset Generation | AI landing page creation for no-website leads |
| M7 | Conversion Tracking Loop | Phase 4.1 — Feedback & Optimization Loop | Post-deployment analytics + closed-loop learning |
| M8 | Full Automation Mode | Phase 4.2 — Autonomous Orchestration Layer | Scheduler, monitoring, self-healing runs |

### Alignment Notes

- **Phase 2.x** milestones are mandatory stability gates before any monetization-facing features.
- **Phase 3.x** introduces revenue-generating capabilities (landing pages + outbound workflows).
- **Phase 4.x** is optimization and scale — not required for MVP monetization.

This table is the authoritative mapping between roadmap execution order and architecture specification sections.

