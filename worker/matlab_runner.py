"""Next implementation step: claim a queued task, run MATLAB, upload result.mat to Master."""

# Deliberately separate from the HTTP service so runner recovery and MATLAB failures
# cannot take down the Worker API. The task contract is defined by worker/app.py.
