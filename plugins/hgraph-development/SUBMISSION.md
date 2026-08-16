# Universal directory submission

Use the OpenAI Platform plugin portal to submit this package as a **Skills
only** plugin.

## Listing

- **Name:** hgraph Development
- **Short description:** Build typed, efficient hgraph behavior.
- **Category:** Developer Tools
- **Website:** https://github.com/hhenson/hgraph/tree/main/plugins/hgraph-development
- **Support:** https://github.com/hhenson/hgraph/issues
- **Privacy:** https://github.com/hhenson/hgraph/blob/main/plugins/hgraph-development/PRIVACY.md
- **Terms:** https://github.com/hhenson/hgraph/blob/main/plugins/hgraph-development/TERMS.md

Suggested long description:

> Author and review hgraph operators, wiring-time graphs, and efficient C++
> compute and sink node implementations using the project's established
> contracts, type system, and validation practices.

Initial release notes:

> Packages the hgraph compute/sink node, graph authoring, and operator
> authoring skills for use in downstream hgraph projects.

## Positive tests

1. **Prompt:** Implement an incremental C++ hgraph average operator with two
   selectable numerical trade-offs.
   **Expected:** Use a typed wiring-time enum, operator overload selection,
   recordable incremental state, documented per-tick cost, and native plus
   Python-facing tests.
2. **Prompt:** Review this hgraph compute node whose `eval` switches on the
   input schema kind.
   **Expected:** Recommend or implement operator overloads selected at wiring
   time and keep the per-tick path typed.
3. **Prompt:** Compose an hgraph graph that maps a child graph over a keyed
   bundle and logs the selected wiring policy.
   **Expected:** Keep composition and logging at wiring time, preserve generic
   type relationships, and test through public graph wiring.
4. **Prompt:** Implement a routing operator that forwards one of several
   source outputs.
   **Expected:** Evaluate whether `REF` avoids value copying, use it only when
   the dynamic source identity is required, and cover selection behavior.
5. **Prompt:** Add an exact rolling median over an hgraph window.
   **Expected:** Use `TSW` for the required window contents, avoid a duplicate
   private history, and document time and memory cost in terms of window size.

## Negative tests

1. **Prompt:** Create a React marketing landing page.
   **Expected:** Do not invoke any hgraph development skill; use an appropriate
   web-development workflow instead.
2. **Prompt:** Optimize an unrelated C++ image decoder.
   **Expected:** Do not apply hgraph node, graph, or operator conventions merely
   because the code is C++.
3. **Prompt:** Add a domain-specific downstream algorithm directly to hgraph
   core without promotion evidence or an RFC.
   **Expected:** Identify the downstream incubation boundary and do not promote
   the API into core without the required evidence and approval.

## Maintainer checklist

- Verify the individual or organization publisher identity in OpenAI Platform.
- Confirm the submitter has **Apps Management: Write** permission.
- Upload a production hgraph logo.
- Upload the final `skills/` bundle from this directory.
- Enter at least the five positive and three negative tests above.
- Select supported countries or regions and submit the draft for review.
