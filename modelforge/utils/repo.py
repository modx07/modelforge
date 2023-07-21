import importlib
import os

def get_github_file_url(remote_url, file_path, commit_sha):
    # Remove '.git' from the remote URL, if present
    if remote_url.endswith('.git'):
        remote_url = remote_url[:-4]
    
    # Create the direct link to the file
    file_url = f"{remote_url}/blob/{commit_sha}/{file_path}"
    return file_url

def get_gitlab_file_url(remote_url, file_path, commit_sha):
    # Remove '.git' from the remote URL, if present
    if remote_url.endswith('.git'):
        remote_url = remote_url[:-4]
    
    # Create the direct link to the file
    file_url = f"{remote_url}/-/blob/{commit_sha}/{file_path}"
    return file_url

def check_repo(repo,file,url,name):
    if repo.is_dirty():
        raise ValueError(f'Module "{file}" has uncommitted changes. Please commit changes and push to the remote repository before updating the registry.')
    if not repo.remotes.origin.url == url:
        raise ValueError(f'Remote URL of module "{file}" does not match the URL of the remote repository for the existing model "{name}". Please push changes to the correct repository before updating the registry.')

    origin = repo.remote('origin')
    origin.fetch()

    up_to_date = repo.active_branch.tracking_branch().commit == repo.active_branch.commit

    if not up_to_date:
        raise ValueError(f'Module "{file}" is not up to date with the latest version on the remote repository. Please pull changes and update to the latest version before adding to the registry.')
        
def check_class(file, class_name, class_type):
    try:
        module_name = file.split('/')[-1].split('.py')[0]
        spec = importlib.util.spec_from_file_location(module_name, file)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        cls = getattr(module, class_name)
        if not issubclass(cls, class_type):
            raise TypeError(f'Class "{class_name}" in module "{file}" is not a subclass of {class_type.__name__}.')
    except ModuleNotFoundError:
        raise ModuleNotFoundError(f'Module "{file}" not found.')
    except AttributeError:
        raise AttributeError(f'Class "{class_name}" not found in module "{file}".')
