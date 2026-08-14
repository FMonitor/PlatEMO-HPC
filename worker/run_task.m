function run_task(taskJsonPath, workDir)
%RUN_TASK Run one algorithm/problem batch using a configured local cluster.
arguments
    taskJsonPath (1,1) string
    workDir (1,1) string
end
task = jsondecode(fileread(taskJsonPath));
if ~isfolder(workDir), mkdir(workDir); end
settingsBaseline = loadSettingsBaseline(task);
if ~isfield(task, 'seeds') || isempty(task.seeds)
    error('PlatEMO:HPC:InvalidBatch', 'A batch task requires a nonempty seeds array');
end
seeds = double(task.seeds(:)');
if any(~isfinite(seeds) | seeds ~= floor(seeds)) || numel(unique(seeds)) ~= numel(seeds)
    error('PlatEMO:HPC:InvalidBatch', 'seeds must be unique integers');
end
task.total_runs = numel(seeds);
task.runs_dir = char(fullfile(workDir, 'runs'));
if ~isfolder(task.runs_dir), mkdir(task.runs_dir); end
progressPath = fullfile(workDir, 'progress.json');
states = repmat(struct('seed', 0, 'state', 'queued', 'fe', 0, 'total_fe', getField(task, 'max_fe', 0), ...
    'elapsed_seconds', 0, 'error', ''), 1, numel(seeds));
for i = 1:numel(seeds), states(i).seed = seeds(i); end
profile = char(getField(task, 'cluster_profile', 'local'));
pool = gcp('nocreate');
if ~isempty(pool), delete(pool); end
pool = parpool(profile);
poolCleanup = onCleanup(@() cleanupPool(pool));
queue = parallel.pool.DataQueue;
afterEach(queue, @applyUpdate);
writeProgress('running');
records = cell(1, numel(seeds));
parfor i = 1:numel(seeds)
    records{i} = run_batch_seed(task, seeds(i), queue);
end
for i = 1:numel(records)
    applyUpdate(records{i});
end
writeProgress('completed');
% Each seed MAT is the durable result artifact. The Worker uploads these
% files individually, so no aggregate result.mat is created.

    function applyUpdate(update)
        index = find([states.seed] == double(update.seed), 1);
        if isempty(index), return; end
        fields = fieldnames(update);
        for f = 1:numel(fields), states(index).(fields{f}) = update.(fields{f}); end
        writeProgress('running');
    end

    function writeProgress(phase)
        completed = sum(strcmp({states.state}, 'completed'));
        failed = sum(strcmp({states.state}, 'failed'));
        running = sum(strcmp({states.state}, 'running'));
        payload = struct('phase', phase, 'completed_runs', completed, 'failed_runs', failed, ...
            'running_runs', running, 'total_runs', numel(states), 'runs', states, ...
            'pool', struct('active', true, 'type', class(pool), 'workers', pool.NumWorkers, ...
                'cluster_profile', profile), 'timestamp', char(datetime('now', 'TimeZone', 'UTC')));
        temporary = [char(progressPath) '.tmp'];
        fid = fopen(temporary, 'w');
        if fid < 0, error('PlatEMO:HPC:Progress', 'Cannot write progress.json'); end
        cleanup = onCleanup(@() fclose(fid));
        fprintf(fid, '%s', jsonencode(payload));
        clear cleanup
        movefile(temporary, char(progressPath), 'f');
    end
end

function baseline = loadSettingsBaseline(task)
% The Master has already expanded Settings into an explicit assignment. The
% local MAT is a verified compatibility baseline, never an override for it.
baseline = struct('applied', false, 'path', '', 'sha256', '');
if ~isfield(task, 'settings_file_path') || isempty(task.settings_file_path), return; end
path = char(task.settings_file_path);
if ~isfile(path), error('PlatEMO:HPC:Settings', 'Settings MAT is unavailable: %s', path); end
source = load(path);
if ~isfield(source, 'Setting') || ~iscell(source.Setting) || numel(source.Setting) < 2
    error('PlatEMO:HPC:Settings', 'Settings MAT must contain Setting{1} and Setting{2}');
end
algorithmNames = settingNames(source.Setting{1});
problemNames = settingNames(source.Setting{2});
if ~ismember(string(task.algorithm.name), algorithmNames) || ~ismember(string(task.problem.name), problemNames)
    error('PlatEMO:HPC:Settings', 'Settings MAT does not include assigned algorithm/problem');
end
baseline.applied = true;
baseline.path = path;
if isfield(task, 'settings_sha256'), baseline.sha256 = char(task.settings_sha256); end
end

function names = settingNames(value)
if iscell(value)
    names = strings(1, numel(value));
    for index = 1:numel(value), names(index) = string(value{index}); end
else
    names = string(value(:)');
end
names = erase(names, '.m');
end

function value = getField(s, name, fallback)
if isstruct(s) && isfield(s, name), value = s.(name); else, value = fallback; end
end

function cleanupPool(pool)
if ~isempty(pool) && isvalid(pool), delete(pool); end
end
