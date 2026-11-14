---
name: coding-guidelines
description: Review code for quality, error handling, logging, configuration, testing, security, and scope discipline. Report actionable findings and recommend commit messages or pull request descriptions without editing code, staging files, committing, pushing, or opening pull requests.
---

# Coding Guidelines

## Intent

Keep code consistent, reviewable and cheap to maintain, and catch the predictable review comments before a reviewer has to write them.

## Review-only boundaries

- This skill authorizes review and recommendations only, not implementation or Git changes.
- Never stage files, create commits, push, create branches, rewrite history, or open pull requests as part of this skill. Commit messages, branch names, and pull request descriptions below are suggestions for the user to apply manually.
- Do not edit, delete, revert, stash, or format files in place. Preserve all existing work. Interpret instructions to fix, delete, or change code below as recommendations, not permission to make changes.
- Read-only Git inspection is allowed. Run formatters in check mode and linters without automatic fixes. Run existing tests and type checks where available; report missing tools or checks that could not be run.
- Do not revert a fix for mutation testing during a review. Recommend an isolated mutation test and state when it has not been performed.
- Return prioritized findings with file references, observed impact, recommended changes, validation results, and suggested commit messages. Do not claim a finding was fixed or a check passed without evidence.

## Context

[Coding conventions](https://en.wikipedia.org/wiki/Coding_conventions) are guidelines covering file organization, naming, comments, declarations and programming practice. Maintenance is the most cited reason for them: a large share of the lifetime cost of software is maintenance, and consistent code is easier to read, review and change by someone who did not write it. Conventions are not enforced by compilers. They are enforced by team agreement, [code review](https://en.wikipedia.org/wiki/Code_review) and static analysis, so run the automated checks before asking a human to review.

## Core tenets

- **Single source of truth and clear ownership.** Keep each rule in one place, and generate or test any copies. Give each piece of knowledge an owner.
- **Depend on contracts.** Use stable interfaces, not internals or file layouts.
- **Normalize at the boundary.** Convert external input into one internal form. Make lossy conversions explicit.
- **Make invariants and defaults explicit.** Missing, empty, null and invalid are not the same thing unless the contract says so.
- **Validate pre- and postconditions.**
- **Keep it simple.** Use the smallest design that meets the real requirements, with no speculative abstractions.
- **Make drift hard.** Automate checks so related definitions can't diverge.
- **Resilience in proportion.** Expect dependencies to fail, and keep recovery logic proportional to its benefit.
- **Resolve once, reuse.** Work out stable properties once and reuse the result.
- **Optimize for change.** Separate new, existing and rejected behavior.
- **Test behavior at the boundary.** Test what neighbours observe, not just internal helpers.

## Structure

**Code quality**
- Keep functions small and single purpose. Name things by what they are, not how they are used today.
- Delete dead code and unused imports. Code with no production caller should not ship.
- Prefer the standard library and existing helpers to new ones.
- Keep a change inside its stated scope. Unrelated cleanup goes in a separate change.

**Error handling**
- Catch the specific exceptions you expect, each one named. Never use a bare catch.
- Library code lets unexpected errors propagate. Only top level entry points may catch broadly, and then they log and exit cleanly.
- Never swallow an error silently. Log it with context or re-raise it.
- Every state machine needs a terminal state and a recovery path for failure.

**Logging**
- Use the logging facility, not print statements.
- Log what happened and with which identifiers. Never log secrets or whole request bodies.

**Configuration**
- Read configuration from one place with explicit defaults.
- A switch with a security effect must fail closed.

**Testing**
- Test behaviour, not implementation. Cover the failure path, not only the happy path.
- Mutation test a fix before calling it covered: revert the fix and confirm the new test fails.
- A green suite proves what it checks. Check also what it cannot see: removed callers, renamed names, and prior behaviour that changed.

**Security**
- Validate input at the boundary. Never build commands or queries by string concatenation.
- Keep secrets out of source and out of logs.

**Disclosure**
- State any change in prior behaviour in the commit message and the pull request description.

## Mechanics

When reviewing readiness for a user-managed commit, run these checks in order without modifying source files:
1. Formatter in check mode and linter without automatic fixes.
2. Type checker, if available.
3. Unit tests for the touched area.
4. A search for leftovers: debug prints, commented code, stale names after a rename.

Report the results; do not stage, commit, or push afterward.

**Suggested commit message.** Use `type(scope): short imperative summary` in lowercase, consistent with this repository. Recommend a body explaining why when useful, and reference a ticket only if one was provided. Never invent a ticket or execute the commit.

**Branch name.** Type, ticket and a short slug.

**Pull request description.** In this order: problem, solution, how to test, risks. Keep it short enough to read in a minute.

**Answering a review comment.** Fix it at the level of the rule the comment states, not only at the line it names. Search the change for other instances of the same pattern.

## Benefits

- Fewer review rounds and shorter ones.
- New contributors learn the rules from one place.

## Costs

- Rules need upkeep as the code and tools change.
- Over applying a rule to unrelated code violates scope discipline.

## Common Mistakes

- Fixing the named line and missing identical lines elsewhere.
- Adding a defensive catch that hides the real failure.
- Large mixed changes that are hard to review.
- Declaring a fix tested without proving the test fails without it.

## Testing Considerations

- Recommend verifying the full check sequence on a clean checkout. Do not clean, reset, stash, or switch the user's working tree to obtain one; report the working-tree context and any verification limitations.

## Related

- Security guidelines, for authentication, secrets and exposure.
- Reviewer, which predicts the comments a change will draw.
