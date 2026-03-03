# AGENTS Hard Rules for QuantAda

## 0) Scope
This file defines non-negotiable engineering rules for all coding agents working in this repo.
If a proposed change conflicts with these rules, reject or redesign it.

## 1) Core Principles (Non-Negotiable)
1. High Self-Healing First
- Prefer recovery over perfection: reconnect, retry, reconcile, alarm.
- Runtime failures must degrade safely and continue when possible.

2. Stateless First
- Broker reality is source of truth.
- Do NOT introduce cross-K intent memory queues for buy retry or deferred execution.
- Do NOT reintroduce old deferred/buffered mechanisms.

3. Minimal Change First
- Implement the smallest effective fix.
- Avoid adding new switches/knobs unless required by clear operational need.
- Prefer local, targeted edits over broad refactors.

4. Execution Discipline
- Keep behavior deterministic and auditable.
- Follow existing execution semantics consistently (sellability guard, immediate downgrade retry, daily cleanup policy).

## 2) Architecture Contracts (Must Follow)
1. Respect base interfaces and contracts:
- `live_trader/adapters/base_broker.py`
- `strategies/base_strategy.py`
- `stock_selectors/base_selector.py`
- `risk_controls/base_risk_control.py`

2. Broker-side hard constraints:
- Pending orders contract includes `id` in `get_pending_orders`.
- Implement `cancel_pending_order(order_id)` with safe failure behavior (False instead of crash).
- No local fake cash/position as long-lived source of truth.

3. Order-state semantics:
- Rejected BUY: immediate same-bar downgrade retry path is preferred.
- Multi-symbol retries must be independent.
- A-share/T+1 markets must use sellable semantics, not total position only.

4. Overnight pending order policy:
- Live run performs overnight cleanup before refresh unless `KEEP_OVERNIGHT_ORDERS=True`.
- Cleanup may retry; failures must be logged and alarmed.

## 3) Forbidden Patterns
- Reintroducing `_deferred_orders`, `_buffered_rejected_retries`, or similar queue replay design.
- Persisting stale intent to force next-day replay of prior-day buy decisions.
- Expanding state machines without explicit failure evidence and tests.

## 4) Fast-Generation Workflow (Mandatory)
When user asks for rapid code generation or new module scaffolding, agents must follow this order:
1. Read relevant `vibe_coding_prompts/*` first (broker/strategy/debug_fix/etc.).
2. Then read corresponding base class interface(s).
3. Generate code strictly against prompt constraints + base contracts.
4. If prompt and code diverge, align to current code/tests, then update prompt docs in same change.

## 5) Testing and Verification Discipline
1. Every behavioral change must include focused tests or updated assertions.
2. Always run targeted tests first, then run broader regression when feasible.
3. Report what was validated and what was not validated.

## 6) Communication Style for Agents
- Be concise, direct, and pragmatic.
- Prioritize actionable outcomes over long theory.
- Challenge complexity creep politely; default to simpler robust design.

## 7) Decision Ownership
- AI can propose and rank with high weight.
- Final GO/HOLD/KILL decisions remain human-owned.
