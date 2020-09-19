import argparse, json, subprocess

PROJECT_ID = 'trading-290017'

def _run_command(command, shell=False):
    print(' '.join(command))
    result = subprocess.run(command, stdout=subprocess.PIPE, shell=shell)
    std = str(result.stdout.decode('utf8')).split('\n') if result.stdout is not None else []
    err = str(result.stderr.decode('utf8')).split('\n') if result.stderr is not None else []
    return std + err

def run_build(project_id):
    return _run_command([
        'gcloud',
        'builds',
        'submit',
        '--tag',
        'gcr.io/{project_id}/us_finance_daily_miner'.format(project_id=project_id),
        '--project={project_id}'.format(project_id=project_id)])

def run_deploy(project_id, env_vars_dict):
    env_vars_list = ','.join([k + '=' + env_vars_dict[k] for k in env_vars_dict.keys()])
    return _run_command([
        'gcloud',
        'beta',
        'run',
        'deploy',
        'us-finance-daily-miner',
        '--image',
        'gcr.io/{project_id}/us_finance_daily_miner'.format(project_id=project_id),
        '--project={project_id}'.format(project_id=project_id),
        '--update-env-vars', env_vars_list
    ])

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('target', type=str, help='bulid or deploy')
    args = parser.parse_args()

    project_id = PROJECT_ID

    if args.target == 'build':
        run_build(project_id)
    elif args.target == 'deploy':
        env_vars_dict = json.load(open('env_variables.prod.json'))
        run_deploy(project_id, env_vars_dict)

