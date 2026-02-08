# Execution Checklist — SchedulingAgent + Builder.io Integration

This checklist governs execution of:
- Phase A: Provider-agnostic SchedulingAgent
- Phase B: Builder.io landing page integration

Rules:
- No architecture redesign
- No agent renaming unless explicitly stated
- All changes must be deterministic
- Tests required for every agent or contract
- Existing pipeline behavior must remain unchanged unless specified

---

## Phase A — SchedulingAgent (Capability Inference Only)

### A1. Define Scheduling Contract
- [ ] Create `core/contracts/scheduling.py`
- [ ] Define `SchedulingCapability` schema
- [ ] Ensure schema is provider-agnostic
- [ ] No defaults that imply vendor behavior
- [ ] Scheduling block is optional at lead root

### A2. Implement SchedulingAgent
- [ ] Create `pipelines/maps_web_missing/agents/scheduling_agent.py`
- [ ] Input: enriched leads
- [ ] Output: enriched leads + optional `scheduling` block
- [ ] Deterministic heuristics only
- [ ] No API calls, no env vars, no timestamps

### A3. SchedulingAgent Tests
- [ ] Create `tests/test_scheduling_agent.py`
- [ ] Healthcare lead → scheduling.required = true
- [ ] Non-service lead → no scheduling block
- [ ] Re-run produces identical output
- [ ] Schema validation enforced

### A4. Wire SchedulingAgent
- [ ] Insert agent after `EnrichmentAggregatorAgent`
- [ ] Before `LeadFormatterAgent`
- [ ] Confirm routing behavior unchanged
- [ ] Confirm exports unchanged

---

## Phase B — Builder.io Integration (Landing Pages)

### B1. Lock Landing Page Contract v2
- [ ] Create `core/contracts/landing_page_v2.py`
- [ ] Extend existing landing contract
- [ ] Add `integrations.chat` block
- [ ] Add `integrations.scheduling` block
- [ ] Preserve UUID5 idempotency

### B2. Builder.io Client Adapter
- [ ] Create `core/integrations/builder_io_client.py`
- [ ] Implement create/update content entry
- [ ] No business logic
- [ ] No template logic
- [ ] Accept full payload, return content ID + URL

### B3. Upgrade LandingPageGeneratorAgent
- [ ] Replace HTML output with Builder.io payload generation
- [ ] Map images → Builder Image API
- [ ] Map scheduling → integrations.scheduling.enabled
- [ ] Map chat → integrations.chat.enabled
- [ ] Preserve idempotent regeneration

### B4. Image Handling (Minimal v1)
- [ ] Use Maps images if present
- [ ] Else use social images if present
- [ ] Else pass empty list
- [ ] No scraping in this phase

### B5. Builder Integration Tests
- [ ] Create `tests/test_landing_builder_integration.py`
- [ ] Validate payload schema
- [ ] Validate UUID5 idempotency
- [ ] Validate scheduling/chat toggles
- [ ] Builder API failure does not break pipeline

### B6. Event Emission
- [ ] Emit `landing.generated`
- [ ] Emit `landing.updated`
- [ ] Include has_scheduling / has_chat flags

---

## Final Validation
- [ ] Run full pipeline (single city)
- [ ] Confirm no regression in exports
- [ ] Confirm landing pages render correctly
- [ ] Confirm scheduling metadata propagates
