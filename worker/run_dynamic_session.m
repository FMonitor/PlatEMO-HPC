function run_dynamic_session(inboxDir,outboxDir,runsDir,profile)
% Persistent local parpool supervisor. MATLAB only computes; Python owns I/O.
if ~isfolder(inboxDir), mkdir(inboxDir); end
if ~isfolder(outboxDir), mkdir(outboxDir); end
if ~isfolder(runsDir), mkdir(runsDir); end
cancelDir = fullfile(fileparts(inboxDir),'cancel');
if ~isfolder(cancelDir), mkdir(cancelDir); end
pool = ensurePool(profile);
writeSession(fullfile(fileparts(inboxDir),'session.json'),pool);
cleanup = onCleanup(@() cleanupPool(pool)); %#ok<NASGU>
entries = struct('future',{},'task',{});
while true
    % The local profile may close an idle pool (the default is commonly
    % 30 minutes). Recreate it before accepting more inbox work. Keeping
    % the JSON file in inbox until parfeval succeeds makes this retryable.
    if isempty(pool) || ~isvalid(pool)
        try
            pool = ensurePool(profile);
            writeSession(fullfile(fileparts(inboxDir),'session.json'),pool);
        catch ME
            deleteSession(fullfile(fileparts(inboxDir),'session.json'));
            pause(1);
            continue;
        end
    end
    files = dir(fullfile(inboxDir,'*.json'));
    for i = 1:numel(files)
        source = fullfile(files(i).folder,files(i).name);
        try
            task = jsondecode(fileread(source));
            future = parfeval(pool,@run_dynamic_seed,1,task,outboxDir);
            % The assignment has been copied into the future closure. Do not
            % retain one .accepted marker per Seed in the inbox directory.
            delete(source);
            entries(end+1).future = future; %#ok<AGROW>
            entries(end).task = task;
        catch ME
            % A pool can disappear between the health check and parfeval.
            % Leave the original inbox file in place and rebuild on the
            % next loop instead of converting a transient pool outage into
            % a permanent Seed failure.
            if isPoolFailure(ME)
                pool = [];
                deleteSession(fullfile(fileparts(inboxDir),'session.json'));
                break;
            end
            task = taskFromFile(source,erase(files(i).name,'.json'));
            writeTerminalEvent(outboxDir,task,'failed',0,0,0,getReport(ME,'basic','hyperlinks','off'));
            if isfile(source), delete(source); end
        end
    end
    for i = numel(entries):-1:1
        task = entries(i).task;
        cancelFile = fullfile(cancelDir,[char(task.attempt_id) '.cancel']);
        if isfile(cancelFile)
            cancel(entries(i).future);
            delete(cancelFile);
            writeTerminalEvent(outboxDir,task,'cancelled',0,getField(task,'max_fe',0),0,'cancelled by Master');
            entries(i) = [];
        elseif strcmp(entries(i).future.State,'finished')
            try
                fetchOutputs(entries(i).future);
            catch ME
                writeTerminalEvent(outboxDir,task,'failed',0,getField(task,'max_fe',0),0,getReport(ME,'basic','hyperlinks','off'));
            end
            entries(i) = [];
        end
    end
    pause(0.2);
end
end

function pool = ensurePool(profile)
pool = gcp('nocreate');
if isempty(pool) || ~isvalid(pool)
    pool = parpool(profile);
end
% Prevent the default profile idle timeout from closing a long-running
% supervisor between Seed assignments. Older MATLAB releases may not
% expose this property, so keep the explicit health/restart fallback.
try
    if isprop(pool,'IdleTimeout')
        pool.IdleTimeout = Inf;
    end
catch
end
end

function deleteSession(path)
if isfile(path), delete(path); end
end

function tf = isPoolFailure(exception)
message = lower(getReport(exception,'basic','hyperlinks','off'));
tf = contains(message,'parpool') || contains(message,'parfeval') || ...
    contains(message,'parallel.pool') || contains(message,'processpool') || ...
    contains(message,'pool is closed') || contains(message,'pool has been closed');
end

function result = run_dynamic_seed(task,outboxDir)
attemptId = char(task.attempt_id); seed = double(task.seed); started = tic;
lastReportedFE = -1;
progressIntervalFE = max(1,double(getField(task,'progress_interval_fe',max(1,floor(task.max_fe/100)))));
result = true; task.runs_dir = char(task.run_dir);
if ~isfolder(task.runs_dir), mkdir(task.runs_dir); end
try
    workerDir = fileparts(mfilename('fullpath')); addpath(genpath(fileparts(workerDir)));
    settingsBaseline = loadSettingsBaseline(task); %#ok<NASGU>
    task.settings_baseline = settingsBaseline;
    rng(seed,'twister');
    args = {'algorithm',algorithmValue(task),'problem',problemHandle(task.problem), ...
        'maxFE',task.max_fe,'save',max(1,getField(task,'retain_points',1)),'outputFcn',@progress};
    for key = {'N','M','D'}
        if isfield(task,key{1}), args = [args,{key{1},task.(key{1})}]; end %#ok<AGROW>
    end
    if isfield(task,'problem_parameter_values') && ~isempty(task.problem_parameter_values)
        args = [args,{'parameter',asParameterCell(task.problem_parameter_values)}]; %#ok<AGROW>
    end
    platemo(args{:});
    artifactPath = fullfile(task.runs_dir,[attemptId '.mat']);
    if ~isfile(artifactPath)
        error('PlatEMO:HPC:MissingSeedArtifact','PlatEMO completed without writing the Seed MAT');
    end
    writeTerminalEvent(outboxDir,task,'completed',task.max_fe,task.max_fe,toc(started),'',artifactPath);
catch ME
    writeTerminalEvent(outboxDir,task,'failed',0,getField(task,'max_fe',0),toc(started),getReport(ME,'basic','hyperlinks','off'));
end
    function progress(Algorithm,Problem)
        if Problem.FE >= Problem.maxFE
            % Keep the uploaded artifact in PlatEMO's traditional format.
            % Worker/task metadata is transported by the outbox and API.
            result = Algorithm.result; %#ok<NASGU>
            metric = Algorithm.metric; %#ok<NASGU>
            artifactPath = fullfile(task.runs_dir,[attemptId '.mat']);
            save(artifactPath,'result','metric','-v7');
        end
        if Problem.FE >= Problem.maxFE || lastReportedFE < 0 || Problem.FE-lastReportedFE >= progressIntervalFE
            lastReportedFE = Problem.FE;
            writeEvent(outboxDir,makeEvent(task,'running',Problem.FE,Problem.maxFE,toc(started),''));
        end
    end
end

function task = taskFromFile(path,attemptId)
task = struct('attempt_id',attemptId,'lease_token','','experiment_point_id','','seed',0,'max_fe',0);
if isfile(path)
    try
        task = jsondecode(fileread(path));
    catch
    end
end
end

function writeTerminalEvent(outboxDir,task,state,fe,totalFE,elapsed,errorText,artifactPath)
if nargin < 8, artifactPath = ''; end
event = makeEvent(task,state,fe,totalFE,elapsed,errorText);
if ~isempty(artifactPath), event.artifact_path = artifactPath; end
writeEvent(outboxDir,event);
end

function event = makeEvent(task,state,fe,totalFE,elapsed,errorText)
event = struct('attempt_id',char(task.attempt_id),'lease_token',char(getField(task,'lease_token','')), ...
    'experiment_point_id',char(getField(task,'experiment_point_id','')),'seed',double(getField(task,'seed',0)), ...
    'state',state,'fe',fe,'total_fe',totalFE,'elapsed_seconds',elapsed,'error',errorText);
end

function writeEvent(outboxDir,event)
% Keep one replaceable progress snapshot per Seed. Terminal events have a
% separate stable name so a slow network cannot create thousands of files.
if strcmp(char(event.state),'running')
    name = [char(event.attempt_id) '.progress.json'];
else
    name = [char(event.attempt_id) '.json'];
end
target = fullfile(outboxDir,name); temporary = [target '.tmp'];
fid = fopen(temporary,'w'); fprintf(fid,'%s',jsonencode(event)); fclose(fid); movefile(temporary,target,'f');
end

function writeSession(path,pool)
payload = struct('actual_workers',pool.NumWorkers,'profile',pool.Cluster.Profile, ...
    'updated_at',char(datetime('now','TimeZone','UTC')));
temporary = [path '.tmp']; fid = fopen(temporary,'w');
fprintf(fid,'%s',jsonencode(payload)); fclose(fid); movefile(temporary,path,'f');
end

function cleanupPool(pool)
if ~isempty(pool) && isvalid(pool), delete(pool); end
end

function handle = problemHandle(entry)
if ischar(entry) || isstring(entry)
    handle = str2func(char(entry));
elseif isstruct(entry) && isfield(entry,'name')
    handle = str2func(char(entry.name));
else
    error('PlatEMO:HPC:InvalidSeed','problem needs a name');
end
end

function value = algorithmValue(task)
entry = task.algorithm;
if ischar(entry) || isstring(entry)
    handle = str2func(char(entry));
elseif isstruct(entry) && isfield(entry,'name')
    handle = str2func(char(entry.name));
else
    error('PlatEMO:HPC:InvalidSeed','algorithm needs a name');
end
if isfield(task,'algorithm_parameter_values') && ~isempty(task.algorithm_parameter_values)
    value = [{handle},asParameterCell(task.algorithm_parameter_values)];
else
    value = handle;
end
end

function values = asParameterCell(entries)
values = cell(1,numel(entries));
for i = 1:numel(entries)
    if ~isfield(entries(i),'value'), error('PlatEMO:HPC:InvalidSeed','parameter entry has no value'); end
    values{i} = entries(i).value;
end
end

function value = getField(s,name,fallback)
if isstruct(s) && isfield(s,name), value = s.(name); else, value = fallback; end
end

function baseline = loadSettingsBaseline(task)
% Settings is a verified compatibility baseline, never an assignment override.
baseline = struct('applied',false,'path','','sha256','');
if ~isfield(task,'settings_file_path') || isempty(task.settings_file_path), return; end
path = char(task.settings_file_path);
if ~isfile(path), error('PlatEMO:HPC:Settings','Settings MAT is unavailable: %s',path); end
source = load(path);
if ~isfield(source,'Setting') || ~iscell(source.Setting) || numel(source.Setting) < 2
    error('PlatEMO:HPC:Settings','Settings MAT must contain Setting{1} and Setting{2}');
end
algorithmNames = settingNames(source.Setting{1});
problemNames = settingNames(source.Setting{2});
algorithmName = char(getField(task.algorithm,'name',''));
problemName = char(getField(task.problem,'name',''));
if ~ismember(string(algorithmName),algorithmNames) || ~ismember(string(problemName),problemNames)
    error('PlatEMO:HPC:Settings','Settings MAT does not include assigned algorithm/problem');
end
baseline.applied = true;
baseline.path = path;
if isfield(task,'settings_sha256'), baseline.sha256 = char(task.settings_sha256); end
end

function names = settingNames(value)
if iscell(value)
    names = strings(1,numel(value));
    for index = 1:numel(value), names(index) = string(value{index}); end
else
    names = string(value(:)');
end
names = erase(names,'.m');
end
