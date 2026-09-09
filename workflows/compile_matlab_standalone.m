function compile_matlab_standalone()
% Compile through upstream entrypoints; package and runtime checks run separately.
request = jsondecode(fileread(getenv('NEURO_MATLAB_REQUEST')));
workspace = getenv('GITHUB_WORKSPACE');
output = getenv('NEURO_MATLAB_OUTPUT');
source = fullfile(workspace, '_matlab_source');
compiled = fullfile(output, 'compiled');
assert(strcmp(['R' version('-release')], request.runtime_release), ...
    'Installed MATLAB release disagrees with the request');
assert(license('test', 'Compiler'), 'MATLAB Compiler entitlement is required');
assert(~exist(compiled, 'dir'), 'Compilation output already exists');
mkdir(compiled);
adjustments = {};

switch request.software
    case 'fieldtrip'
        products = {'signal', 'images', 'stats', 'optim', 'curvefit'};
        for index = 1:numel(products)
            assert(exist(fullfile(matlabroot, 'toolbox', products{index}), 'dir') == 7, ...
                ['Missing toolbox required by ft_compile_standalone: ' products{index}]);
        end
        addpath(source);
        ft_defaults;
        ft_compile_standalone(source);
        copyfile(fullfile(source, 'bin', '*'), compiled);
    case 'physio'
        spmroot = fullfile(workspace, '_matlab_spm');
        physio = fullfile(spmroot, 'toolbox', 'PhysIO');
        assert(~exist(physio, 'dir'), 'SPM checkout already contains a PhysIO toolbox');
        mkdir(physio);
        entries = dir(source);
        for index = 1:numel(entries)
            name = entries(index).name;
            if ~any(strcmp(name, {'.', '..', '.git', '.github'}))
                copyfile(fullfile(source, name), fullfile(physio, name));
            end
        end
        assert(exist(fullfile(physio, 'tapas_physio_cfg_matlabbatch.m'), 'file') == 2, ...
            'PhysIO has no root SPM batch configuration for static compilation');
        addpath(spmroot);
        addpath(genpath(physio));
        assert(strcmp(spm('Ver'), 'SPM12'), 'PhysIO candidate must use SPM12');
        assert(tapas_physio_init(), 'PhysIO SPM batch initialization failed');
        spm_jobman('initcfg');
        helper = fullfile(spmroot, 'config', 'spm_make_standalone.m');
        original = fileread(helper);
        old = 'Nopts = {''-p'',fullfile(matlabroot,''toolbox'',''signal'')};';
        replacement = ['Nopts = {''-p'',fullfile(matlabroot,''toolbox'',''signal''),' ...
            '''-p'',fullfile(matlabroot,''toolbox'',''stats'')};'];
        assert(numel(strfind(original, old)) == 1, ...
            'SPM12 compiler toolbox declaration changed; review the PhysIO adaptation');
        assert(exist(fullfile(matlabroot, 'toolbox', 'stats'), 'dir') == 7, ...
            'PhysIO requires the Statistics and Machine Learning Toolbox');
        file = fopen(helper, 'w');
        assert(file ~= -1, 'Cannot add the PhysIO compiler dependency');
        fprintf(file, '%s', strrep(original, old, replacement));
        fclose(file);
        adjustments = {'SPM12 mcc -p includes Statistics and Machine Learning Toolbox'};
        spm_make_standalone(compiled);
        config = fileread(fullfile(spmroot, 'config', 'spm_cfg_static_tools.m'));
        assert(contains(lower(config), 'physio'), 'Compiled SPM toolbox list omits PhysIO');
    otherwise
        error('Unsupported upstream compilation entrypoint');
end

report.software = request.software;
report.runtime_release = ['R' version('-release')];
report.matlab_root = matlabroot;
report.compiler_adjustments = adjustments;
file = fopen(fullfile(output, 'compiler-report.json'), 'w');
assert(file ~= -1, 'Cannot write compiler report');
fprintf(file, '%s\n', jsonencode(report));
fclose(file);
end
