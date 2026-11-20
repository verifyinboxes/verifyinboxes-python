# Security policy

## Reporting a vulnerability

Report it privately: use GitHub's private vulnerability reporting on this repository (**Security** → **Report a
vulnerability**), or email **support@verifyinboxes.com** with what you found and how to reproduce it. Please do not
open a public issue for a security problem.

We will confirm that we have your report, tell you what we find, and say when a fix is released.

## Supported versions

Fixes go into the latest release.

## API keys and tokens

Never put an API key, a personal access token or a webhook secret in an issue, a pull request or a log you share.
This library never logs them, and shows at most the first 6 characters of a key in `repr()`. If a key has been
exposed, an organisation admin revokes it in the VerifyInboxes app under **Developers** → **API keys** and makes a
new one.
