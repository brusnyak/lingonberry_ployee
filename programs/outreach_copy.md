# Autoresearch Program: Outreach Copy Optimization

## Goal
Improve reply rate for cold outreach emails by making the copy plainer, less salesy,
and more open about the sender starting out. The goal is to sound like a real person,
not like a polished cold-email template.

## Target file
outreach/generator.py

## Metric
Manual — operator reviews reply rate after each batch of 5+ sends.
Baseline: 0 replies from first batch (no sends yet).
Target: at least 1 reply per 10 sends (10% reply rate).

To switch to automated metric once sends are running:
cmd: python agent/metrics/reply_rate.py

## Baseline
0 replies from 0 sends. No baseline established yet.
First experiment should focus on:
- soft, plain-text intros
- honest "I'm starting out" framing when it helps
- one specific thing the sender thinks they could help with
- one low-pressure question at the end

## Constraints
- Do not change the function signature of generate_email()
- Do not remove language support (en/sk/cs/de packs must remain)
- Do not add new external dependencies
- Keep emails under 120 words
- Do not make claims that can't be verified from public signals (see PAIN_SIGNAL_STRATEGY.md)
- Safe claims only: "I think I could help with...", "I noticed...", "Curious if..."
- No fake urgency, no "limited time", no aggressive CTAs
- Prefer plain, open phrasing over clever hooks

## Experiment budget
300
