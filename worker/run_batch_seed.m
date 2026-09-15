function record = run_batch_seed(task, seed, queue)
%RUN_BATCH_SEED Execute one seed on a parpool worker and report FE events.
record = struct('seed', seed, 'state', 'running', 'fe', 0, 'total_fe', task.max_fe, ...
    'elapsed_seconds', 0, 'error', '');
task_snapshot = task;
matlab_version = version;
lastReportedFE = 0;
progressIntervalFE = max(1, double(getField(task, 'progress_interval_fe', max(1, floor(task.max_fe / 100)))));
send(queue, record);
started = tic;
try
    workerDir = fileparts(mfilename('fullpath'));
    platemoRoot = fileparts(workerDir);
    addpath(genpath(platemoRoot));
    algorithm = algorithmValue(task);
    problem = problemHandle(task.problem);
    rng(seed, 'twister');
    args = {'algorithm', algorithm, 'problem', problem, 'maxFE', task.max_fe, ...
        'save', max(1, getField(task, 'retain_points', 1)), 'outputFcn', @reportProgress};
    if isfield(task, 'N'), args = [args, {'N', task.N}]; end
    if isfield(task, 'M'), args = [args, {'M', task.M}]; end
    if isfield(task, 'D'), args = [args, {'D', task.D}]; end
    if isfield(task, 'problem_parameter_values') && ~isempty(task.problem_parameter_values)
        args = [args, {'parameter', asParameterCell(task.problem_parameter_values)}];
    end
    % platemo() forces save=0 whenever outputs are requested. Invoke it
    % without outputs; reportProgress saves Algorithm.result at completion.
    platemo(args{:});
    record.state = 'completed';
    record.fe = task.max_fe;
    record.elapsed_seconds = toc(started);
    % Successful artifacts are written by reportProgress in native PlatEMO
    % result/metric format. Lifecycle metadata is sent through the queue.
catch ME
    record.state = 'failed';
    record.elapsed_seconds = toc(started);
    record.error = getReport(ME, 'basic', 'hyperlinks', 'off');
    save(fullfile(task.runs_dir, sprintf('seed-%d.mat', seed)), 'seed', 'record', 'task_snapshot', 'matlab_version', '-v7');
end

send(queue, record);

    function reportProgress(Algorithm, Problem)
        record.fe = Problem.FE;
        record.total_fe = Problem.maxFE;
        record.elapsed_seconds = toc(started);
        if Problem.FE < Problem.maxFE && Problem.FE - lastReportedFE < progressIntervalFE
            return;
        end
        lastReportedFE = Problem.FE;
        if Problem.FE >= Problem.maxFE
            result = Algorithm.result; %#ok<NASGU>
            metric = Algorithm.metric; %#ok<NASGU>
            save(fullfile(task.runs_dir, sprintf('seed-%d.mat', seed)), 'result', 'metric', '-v7');
        end
        send(queue, record);
    end
end

function handle = problemHandle(entry)
if ischar(entry) || isstring(entry)
    handle = str2func(char(entry));
elseif isstruct(entry) && isfield(entry, 'name')
    handle = str2func(char(entry.name));
else
    error('PlatEMO:HPC:InvalidBatch', 'problem needs a name');
end
end

function values = asParameterCell(entries)
if ~isstruct(entries)
    error('PlatEMO:HPC:InvalidBatch', 'problem_parameter_values must be an array of value entries');
end
values = cell(1, numel(entries));
for i = 1:numel(entries)
    if ~isfield(entries(i), 'value')
        error('PlatEMO:HPC:InvalidBatch', 'problem_parameter_values entry has no value');
    end
    values{i} = entries(i).value;
end
end

function value = algorithmValue(task)
entry = task.algorithm;
if ischar(entry) || isstring(entry)
    handle = str2func(char(entry));
elseif isstruct(entry) && isfield(entry, 'name')
    handle = str2func(char(entry.name));
else
    error('PlatEMO:HPC:InvalidBatch', 'algorithm needs a name');
end
if isfield(task, 'algorithm_parameter_values') && ~isempty(task.algorithm_parameter_values)
    value = [{handle}, asParameterCell(task.algorithm_parameter_values)];
else
    value = handle;
end
end

function value = getField(s, name, fallback)
if isstruct(s) && isfield(s, name)
    value = s.(name);
else
    value = fallback;
end
end
