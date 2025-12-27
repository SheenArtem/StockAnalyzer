---
description: Run local verification tests before pushing to remote
---

1. **Update Version**:
   - Locate `st.caption("Version: ...")` in `app.py`.
   - Update the version number (format: vYYYY.MM.DD.XX).

2. **Run Verification**:
   - **Rule**: You MUST run the local verification script before pushing any code.
   - Command: `python run_local_test.py`
   - 🔴 **IF FAILED**: Stop immediately. Fix it.
   - 🟢 **IF PASSED**: Proceed.

3. **Push**:
   - Command: `git push`
