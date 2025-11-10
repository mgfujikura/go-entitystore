@echo off

SET APP_YAML=.\aememcachestore\test\app.yaml

PUSHD %~dp0\..\..
CALL .\aememcachestore\test\env.cmd
CMD /C gcloud app deploy %APP_YAML% --project=%PROJECT% %*
POPD
