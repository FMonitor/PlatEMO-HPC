function record = run_batch_seed(task, seed, queue)
%RUN_BATCH_SEED Execute one seed on a parpool worker and report FE events.
record = struct('seed', seed, 'state', 'running', 'fe', 0, 'total_fe', task.max_fe, ...
    'elapsed_seconds', 0, 'error', '');
send(queue, record);
started = tic;
try
    workerDir = fileparts(mfilename('fullpath'));
    platemoRoot = fileparts(workerDir);
    addpath(genpath(platemoRoot));
    algorithm = entryValue(task.algorithm);
    problem = entryValue(task.problem);
    rng(seed, 'twister');
    args = {'algorithm', algorithm, 'problem', problem, 'maxFE', task.max_fe, ...
        'save', max(1, getField(task, 'retain_points', 1)), 'outputFcn', @reportProgress};
    if isfield(task, 'N'), args = [args, {'N', task.N}]; end
    if isfield(task, 'M'), args = [args, {'M', task.M}]; end
    if isfield(task, 'D'), args = [args, {'D', task.D}]; end
    [decs, objs, cons] = platemo(args{:});
    record.state = 'completed';
    record.fe = task.max_fe;
    record.elapsed_seconds = toc(started);
    save(fullfile(task.runs_dir, sprintf('seed-%d.mat', seed)), 'seed', 'decs', 'objs', 'cons', 'record', '-v7');
catch ME
    record.state = 'failed';
    record.elapsed_seconds = toc(started);
    record.error = getReport(ME, 'basic', 'hyperlinks', 'off');
    save(fullfile(task.runs_dir, sprintf('seed-%d.mat', seed)), 'seed', 'record', '-v7');
end
send(queue, record);

    function reportProgress(~, Problem)
        record.fe = Problem.FE;
        record.total_fe = Problem.maxFE;
        record.elapsed_seconds = toc(started);
        send(queue, record);
    end
end

function value = entryValue(entry)
if ischar(entry) || isstring(entry)
    value = str2func(char(entry));
elseif isstruct(entry) && isfield(entry, 'name')
    handle = str2func(char(entry.name));
    if isfield(entry, 'parameters') && ~isempty(entry.parameters)
        if iscell(entry.parameters), parameters = entry.parameters;
        elseif isstruct(entry.parameters), parameters = struct2cell(entry.parameters)';
        else, parameters = {entry.parameters}; end
        value = [{handle}, parameters];
    else
        value = handle;
    end
else
    error('PlatEMO:HPC:InvalidBatch', 'algorithm and problem need a name');
end
end

function value = getField(s, name, fallback)
if isstruct(s) && isfield(s, name), value = s.(name); else, value = fallback; end
end
