# Security Policy

## Supported versions

Only the latest commit on `main` is supported. There are no release branches.

## Reporting a vulnerability

Please do **not** open a public issue for security problems.

Report privately through GitHub: open the **Security** tab of this repository and choose **Report a vulnerability**. Include:

- what you found and where (file, function, request),
- steps or a minimal payload to reproduce it,
- the impact you expect.

You should get an acknowledgement within 5 working days. Confirmed issues are fixed on `main` with a regression test, and the reporter is credited unless they ask otherwise.

## Scope

Tickscope parses untrusted binary input, so memory-safety findings in the parser, feed handler or order book are in scope and especially welcome. It reads synthetic feeds only, opens no sockets and needs no credentials. Every change is built with AddressSanitizer, UndefinedBehaviorSanitizer and ThreadSanitizer in CI.
