---
description: Run local verification tests before pushing to remote
---

1. **Rule**: You MUST run the local verification script before pushing any code.
   Command: `python run_local_test.py`

2. **Check Results**:
   - 🔴 **IF FAILED**: Stop immediately. Do not push. Analyze the error and fix it.
   - 🟢 **IF PASSED**: Proceed to push.

3. **Push**:
   Command: `git push`
