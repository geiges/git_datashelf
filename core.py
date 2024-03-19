#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 19 09:46:48 2024

@author: andreasgeiges
"""
import csv
import os
import time
import pandas as pd
import numpy as np
import git
import tabulate
import traceback

from threading import Thread
from collections import defaultdict
from pathlib import Path

import config


#%% Functions 
def get_time_string():
    """
    Return formated time string.

    Returns
    -------
    time string : str

    """
    return time.strftime("%Y/%m/%d-%I:%M:%S")


def dict_to_csv(dictionary, filePath):

    with open(filePath, 'w', newline='') as file:
        writer = csv.writer(file)
        for key, val in dictionary.items():
            writer.writerow([key, val])



def csv_to_dict(filePath):

    with open(filePath, 'r', newline='') as file:
        reader = csv.reader(file)
        mydict = dict()
        for row in reader:
            print(row)
            #            v = rows[1]
            mydict[row[0]] = row[1]
    return mydict

#%% Git Repository Manager
class GitRepository_Manager:
    """
    # Management of git repositories for fast access
    """

    #%% Magicc methods
    def __init__(self, 
                 path_to_repo,
                 source_file,
                 debugmode=False):
        
        # config
        self.cfg = dict(
            DEBUG= debugmode,
            PATH_TO_DATASHELF = path_to_repo,
            SOURCE_FILE = source_file,
            )
        
        
        self.sources = pd.read_csv(self.cfg.SOURCE_FILE, index_col="SOURCE_ID")

        remote_repo_path = os.path.join(
            self.cfg.PATH_TO_REPO, "remote_sources", "source_states.csv"
        )
        if os.path.exists(remote_repo_path):
            self.remote_sources = pd.read_csv(remote_repo_path, index_col=0)
            
            
            new_items, updated_items = self._get_difference_to_remote()
            n_new_entries = len(new_items)
            n_updated_sources = len(updated_items)
            if n_new_entries + n_updated_sources  > 0:
                print('Remote: ',end='')
                if n_new_entries >0:
                   print(f'({n_new_entries}) new sources', end='')
                    
                if n_updated_sources > 0:
                    print(f' and ({len(updated_items)}) updated sources', end='')
                print(' are available online (see dt.available_remote_data_updates)')
                
        else:
            print('Remote: not setup')
        
        self.repositories = dict()
        self.updatedRepos = set()
        self.validatedRepos = set()
        self.filesToAdd = defaultdict(list)
        
        # remote update checks (only once per day)
        self._init_remote_repo()
        
        
        if not debugmode:
            for sourceID in self.sources.index:
                repoPath = os.path.join(self.PATH_TO_DATASHELF, "database", sourceID)
                self.repositories[sourceID] = git.Repo(repoPath)
                self.verifyGitHash(sourceID)

            self.repositories["main"] = git.Repo(self.PATH_TO_DATASHELF)
            self._validateRepository("main")
        else:
            print("Git manager initialized in debugmode")

        self.check_for_new_remote_data()
        
    def __getitem__(self, sourceID):
        """
        Retrieve `sourceID` from repositories dictionary and ensure cleanliness
        """
        repo = self.repositories[sourceID]
        if sourceID not in self.validatedRepos:
            self._validateRepository(sourceID)
        return repo

    #%% Private methods
    
    def _ssh_agent_running(self):
        import subprocess

        proc = subprocess.Popen(["ssh-add -l"], stdout=subprocess.PIPE, shell=True)
        (out, err) = proc.communicate()
        return not out.startswith(b'The agent has no identities')

    def _get_difference_to_remote(self):
        
        new_items = self.remote_sources.index.difference(
            self.sources.index
        )
        compare_df = self.sources.copy()
        compare_df['remote_tag'] = self.remote_sources['tag']
        compare_df = compare_df[(~compare_df.tag.isnull()) &(~compare_df.remote_tag.isnull())]
        
        updated_items = compare_df.index[(
            compare_df.tag.apply(lambda x : float(x[1:])) < compare_df.remote_tag.apply(lambda x : float(x[1:]))
            )]
        
        
        
        return new_items, updated_items
    
    def _check_online_data(self):
        curr_date = pd.to_datetime(get_time_string()).date()
        last_access_date = pd.to_datetime(
            self._get_last_remote_access()
        ).date()

        return pd.isna(last_access_date) or curr_date > last_access_date
  
    def _init_remote_repo(self):   
        remote_repo_path = os.path.join(self.cfg.PATH_TO_REPO, "remote_sources")
        if os.path.exists(remote_repo_path):
            self.remote_repo = self._get_remote_sources_repo()
            
            dpath = os.path.join(
            self.cfg.PATH_TO_REPO,
            "remote_sources",
            "source_states.csv",
                )
            self.remote_sources = pd.read_csv(dpath, index_col=0)
        else:
            #create empty dummy
            self.remote_sources = pd.DataFrame()
            
    def _get_remote_sources_repo(self):
        remote_repo_path = os.path.join(self.cfg.PATH_TO_REPO, "remote_sources")
        remote_repo = git.Repo(remote_repo_path)

        return remote_repo

    def _pull_remote_sources(self):

        remote_repo_path = os.path.join(self.cfg.PATH_TO_REPO, "remote_sources")
        if os.path.exists(remote_repo_path):
            # pull
            remote_repo_path = os.path.join(self.cfg.PATH_TO_REPO, "remote_sources")
            remote_repo = git.Repo(remote_repo_path)
            remote_repo.remote("origin").pull(progress=TqdmProgressPrinter())

        else:
            # clone
            remote_repo = self._clone_remote_sources()

        self._update_last_remote_access()
        self.remote_repo = remote_repo

        return remote_repo

    def _clone_remote_sources(self):

        url = config.DATASHELF_REMOTE + "remote_sources.git"
        remote_repo = git.Repo.clone_from(
            url=url,
            to_path=os.path.join(self.cfg.PATH_TO_REPO, "remote_sources"),
            progress=TqdmProgressPrinter(),
        )
        self._update_last_remote_access()

        return remote_repo

    def _get_last_remote_access(self):
        filepath = os.path.join(
            self.cfg.PATH_TO_REPO, "remote_sources", "last_accessed_remote"
        )
        if not os.path.exists(filepath):
            return np.nan
        with open(filepath, "r") as f:
            last_accessed = f.read()
        return last_accessed

    def _update_last_remote_access(self):

        filepath = os.path.join(
            self.cfg.PATH_TO_REPO, "remote_sources", "last_accessed_remote"
        )
        with open(filepath, "w") as f:
            f.write(get_time_string())

    def _update_local_sources_tag(self, repoName):
        
        repo = self.repositories[repoName]
        tag = self.get_tag_of_source(repoName)
        self.sources.loc[repoName, "tag"] = tag
        self.commit("Update tags of sources")

    def _update_remote_sources(self, repoName):

        dpath = os.path.join(
            self.cfg.PATH_TO_REPO,
            "remote_sources",
            "source_states.csv",
        )
        remote_repo = git.Repo(os.path.join(self.cfg.PATH_TO_REPO, "remote_sources"))
        rem_sources_df = pd.read_csv(dpath, index_col=0)

        repo = self[repoName]
        hash = repo.commit().hexsha
        user = config.CRUNCHER

        if len(repo.tags) == 0:
            # start new tag with version 1.0
            tag = "v1.0"

        else:
            tags = sorted(repo.tags, key=lambda t: t.commit.committed_datetime)
            last_tag = tags[-1]
            tag_hash = last_tag.commit.hexsha

            if tag_hash == hash:
                # no new commits -> keep tag
                tag = last_tag.name

                if rem_sources_df.loc[repoName, "tag"] == tag:
                    # nothing needs to be done
                    return repo
            else:
                # there are new commits -> increase version by 1.0

                latest_tag = tags[-1]
                tag = f'v{float(latest_tag.name.replace("v",""))+1:1.1f}'

        # update remote sources csv
        repo.create_tag(tag)
        
        rem_sources_df.loc[repoName, :] = (hash, tag, user)
        rem_sources_df.to_csv(dpath)
        
        remote_repo.index.add("source_states.csv")
        remote_repo.index.commit("remote source update" + " by " + config.CRUNCHER)
        
        self.remote_sources = rem_sources_df
        return repo

    def _gitUpdateFile(self, repoName, filePath):
        pass

    def _validateRepository(self, sourceID):
        """
        Private
        Checks if sourceID points to a valid repository

        """
        repo = self.repositories[sourceID]

        if sourceID != "main":
            self.verifyGitHash(sourceID)

        if repo.is_dirty():
            raise RuntimeError(
                'Git repo: "{}" is inconsistent! - please check uncommitted modifications'.format(
                    sourceID
                )
            )

        config.DB_READ_ONLY = False
        if config.DEBUG:
            print("Repo {} is clean".format(sourceID))
        self.validatedRepos.add(sourceID)
        return True

    #%% Public methods
    
    
    def available_remote_data_updates(self):
        
        new_items, updated_items = self._get_difference_to_remote()
        
        print('New items:')
        print(tabulate(
            self.remote_sources.loc[new_items, ['tag', 'last_to_update']], 
            headers="keys", tablefmt="psql"))
        
        print('Sources with newer data:')
        
        df = pd.concat(
            [
                self.sources.loc[updated_items, ['tag']].rename(columns={'tag':'local_tag'}),
                self.remote_sources.loc[updated_items, ['tag']].rename(columns={'tag':'remote_tag'}),
            ], axis=1)
        print(tabulate(
            df, 
            headers="keys", tablefmt="psql"))
    
        
    def check_if_online_repo_exists(self, sourceID):
        """
        Check if source in local inventory

        Parameters
        ----------
        sourceID : TYPE
            DESCRIPTION.

        Returns
        -------
        TYPE
            DESCRIPTION.

        """
        return sourceID in self.remote_sources.index
    
    def check_for_new_remote_data(self, force_check=False, foreground=False):
        """
        Checks if source is in online repositorty

        Parameters
        ----------
        force_check : TYPE, optional
            DESCRIPTION. The default is False.
        foreground : TYPE, optional
            DESCRIPTION. The default is False.

        Returns
        -------
        None.

        """
        if (force_check or self._check_online_data()) and not ("PYTEST_CURRENT_TEST" in os.environ):
                
            # check for remote data
            try:
                if foreground:
                    print("Looking for new online sources...", end='')
                    self._pull_remote_sources()
                    print("Done!")    
                else:
                    if self._ssh_agent_running():
                        print("Looking for new online sources in the backgound")
                        thread = Thread(target=self._pull_remote_sources)
                        thread.start()
                    else:
                        print('SSH agent not running, not checking for remote data.')
                
            except:
                print("Could not check online data repository")
                import traceback

                traceback.print_exc()
         
    def clone_source_from_remote(self, repoName, repoPath):
        """
        Function to clone a remote git repository as a local copy.

        Input
        -----
        repoName : str - valid repository in the remove database
        repoPath : str - path of the repository
        """

        self._pull_remote_sources()
        try:
            print("Try cloning source via ssh...", end='')
            url = config.DATASHELF_REMOTE + repoName + ".git"
            repo = git.Repo.clone_from(
                url=url, to_path=repoPath, progress=TqdmProgressPrinter()
            )
        except:
            print('failed.')
            try:
                
                print("Try Cloning source via https...", end='')
                url = config.DATASHELF_REMOTE_HTTPS + repoName + ".git"
                repo = git.Repo.clone_from(
                    url=url, to_path=repoPath, progress=TqdmProgressPrinter()
                )
            except Exception:
                print('failed.')
                if config.DEBUG:
                    print(traceback.format_exc())
                    print("Failed to import source {}".format(repoName))
                raise(Exception(f"""Both SSH and HTTPs import failed. Check your connection, password or if requrested data exists on remote.
                Consider the following options:                
                    1) Does "{repoName}" exists in {config.DATASHELF_REMOTE_HTTPS}
                    2) Check your ssh connection with: dt.test_ssh_remote_connection())
                    """))
        self.repositories[repoName] = repo

        # Update source file
        sourceMetaDict = csv_to_dict(os.path.join(repoPath, "meta.csv"))
        sourceMetaDict["git_commit_hash"] = repo.commit().hexsha
        tag = self.get_tag_of_source(repoName)
        sourceMetaDict["tag"] = tag
        self.sources.loc[repoName] = pd.Series(sourceMetaDict)
        self.sources.to_csv(config.SOURCE_FILE)
        self.gitAddFile("main", config.SOURCE_FILE)

        return repo
    

    def get_source_repo_failsave(self, sourceID):
        """
        Retrieve `sourceID` from repositories dictionary without checks
        """
        repoPath = os.path.join(self.PATH_TO_DATASHELF, "database", sourceID)
        repo = git.Repo(repoPath)
        return repo

    def get_inventory_file_of_source(self, repoName):
        repo = self[repoName]
        return os.path.join(repo.working_dir, "source_inventory.csv")

    def init_new_repo(self, repoPath, repoID, sourceMetaDict):
        """
        Method to create a new repository for a source

        Input
        ----
        repoPath : str
        repoID   : str
        sourceMetaDict : dict with the required meta data descriptors
        """
        self.sources.loc[repoID] = pd.Series(sourceMetaDict)
        self.sources.to_csv(config.SOURCE_FILE)
        self.gitAddFile("main", config.SOURCE_FILE)

        repoPath = Path(repoPath)
        print(f"creating folder {repoPath}")
        repoPath.mkdir(parents=True, exist_ok=True)
        self.repositories[repoID] = git.Repo.init(repoPath)

        for subFolder in config.SOURCE_SUB_FOLDERS:
            subPath = repoPath / subFolder
            subPath.mkdir(exist_ok=True)
            filePath = subPath / ".gitkeep"
            filePath.touch()
            self.gitAddFile(repoID, filePath)

        metaFilePath = repoPath / "meta.csv"
        dict_to_csv(sourceMetaDict, metaFilePath)
        self.gitAddFile(repoID, metaFilePath)

        self.commit("added source: " + repoID)

    def gitAddFile(self, repoName, filePath):
        """
        Addes a new file to a repository

        Input
        -----
        repoName : str
        filePath : str of the relative file path
        """
        if config.DEBUG:
            print("Added file {} to repo: {}".format(filePath, repoName))

        self.filesToAdd[repoName].append(str(filePath))
        self.updatedRepos.add(repoName)

    def gitRemoveFiles(self, repoName, filePaths):
        """
        Removes mutiple file from the git repository

        Input
        -----
        repoName : str
        filePath : str of the relative file path
        """
        self[repoName].index.remove(filePaths, working_tree=True)
        self.updatedRepos.add(repoName)

    def gitRemoveFile(self, repoName, filePath):
        """
        Removes a file from the git repository

        Input
        -----
        repoName : str
        filePath : str of the relative file path
        """
        if config.DEBUG:
            print("Removed file {} to repo: {}".format(filePath, repoName))
        self[repoName].git.execute(
            ["git", "rm", "-f", filePath]
        )  # TODO Only works with -f forced, but why?
        self.updatedRepos.add(repoName)

    def commit(self, message):
        """
        Function to commit all oustanding changes to git. All repos including
        'main' are commited if there is any change

        Input
        ----
        message : str - commit message
        """
        if "main" in self.updatedRepos:
            self.updatedRepos.remove("main")

        for repoID in self.updatedRepos:
            repo = self.repositories[repoID]
            repo.index.add(self.filesToAdd[repoID])
            commit = repo.index.commit(message + " by " + config.CRUNCHER)
            self.sources.loc[repoID, "git_commit_hash"] = commit.hexsha
            tag = self.get_tag_of_source(repoID)
            self.sources.loc[repoID, "tag"] = tag
            del self.filesToAdd[repoID]

        # commit main repository
        self.sources.to_csv(config.SOURCE_FILE)
        self.gitAddFile("main", config.SOURCE_FILE)

        main_repo = self["main"]
        main_repo.index.add(self.filesToAdd["main"])
        main_repo.index.commit(message + " by " + config.CRUNCHER)
        del self.filesToAdd["main"]

        # reset updated repos to empty
        self.updatedRepos = set()

    def create_remote_repo(self, repoName):
        """
        Function to create a remote git repository from an existing local repo
        """
        repo = self[repoName]

        if "origin" in repo.remotes:
            # remote origin has been configured already, but re-push anyway, since this
            # could have been a connectivity issue
            origin = repo.remotes.origin
        else:
            origin = repo.create_remote(
                "origin", config.DATASHELF_REMOTE + repoName + ".git"
            )

        branch = repo.active_branch
        origin.push(branch, progress=TqdmProgressPrinter())

        # Update references on remote
        origin.fetch()
        branch.set_tracking_branch(origin.refs[0])

    def get_hash_of_source(self, repoName):
        repo = self[repoName]
        return repo.head.commit.hexsha

    def get_tag_of_source(self, repoName):
        repo = self.get_source_repo_failsave(repoName)
        if len(repo.tags) == 0:
            return None

        # tag of head
        return next(
            (tag.name for tag in repo.tags if tag.commit == repo.head.commit), None
        )
        # latest tag
        tags = sorted(repo.tags, key=lambda t: t.commit.committed_datetime)
        return tags[-1].name

    def checkout_git_version(self, repoName, tag):

        repo = self[repoName]

        if tag == "latest":
            # hash = repo.commit().hexsha
            tag = "master"
        elif tag in repo.tags:
            hash = repo.tags["v3.0"].commit.hexsha
        else:
            raise (Exception(f"Tag {tag} does not exist"))

        repo.git.checkout(tag)
        return repo.commit().hexsha

    def push_to_remote_datashelf(self, repoName, force=True):
        """
        This function used git push to update the remote database with an updated
        source dataset.

        Input is the source ID as a str.

        Currently conflicts beyond auto-conflict management are not caught by this
        function. TODO

        """
        remote_repo = self._pull_remote_sources()
        repo = self[repoName]
        if (not force) and (
            "Your branch is up to date with 'origin/master'"
            in repo.git.execute(["git", "status"])
        ):
            print("Nothing to push")
            print(repo.git.execute(["git", "status"]))
            return

        self._update_remote_sources(repoName)
        self._update_local_sources_tag(repoName)

        remote_repo.remotes.origin.push(progress=TqdmProgressPrinter())

        self[repoName].remotes.origin.push(progress=TqdmProgressPrinter())

        self[repoName].remotes.origin.push(progress=TqdmProgressPrinter(), tags=True)

    def test_ssh_remote_connection(self):
        """
        Function to test the ssh connection to the remote data repository using
        'ssh -T host'
        Returns
        -------
        None.

        """
        host = config.DATASHELF_REMOTE.split(':')[0]
        print(f'Testing connection to host {host}')
        cmd = f"ssh -T {host}"
        import subprocess
        retcode = subprocess.call(cmd,shell=True)
        if retcode==0:
            print('Successfully connected')
        else:
            print(f'Connection failed with exit code {retcode}')
    

    def pull_update_from_remote(self, repoName, old_inventory):
        """
        This function used git pull an updated remote source dataset to the local
        database.

        Input is the source ID as a str.

        Currently conflicts beyond auto-conflict management are not caught by this
        function. TODO

        """
        remote_repo = self._pull_remote_sources()

        self[repoName].remote("origin").pull(progress=TqdmProgressPrinter())
        self.updateGitHash_and_Tag(repoName)
        repoPath = os.path.join(self.PATH_TO_DATASHELF, "database", repoName)
        sourceInventory = pd.read_csv(
            os.path.join(repoPath, "source_inventory.csv"),
            index_col=0,
            dtype={"source_year": str},
        )
        new_inventory = pd.concat(
            [old_inventory[old_inventory["source"] != repoName], sourceInventory]
        )

        return new_inventory

    def verifyGitHash(self, repoName):
        """
        Function to verify the git hash code of an existing git repository
        """
        repo = self.repositories[repoName]
        if repo.commit().hexsha != self.sources.loc[repoName, "git_commit_hash"]:
            raise RuntimeError(
                "Source {} is inconsistent with overall database".format(repoName)
            )

    def updateGitHash_and_Tag(self, repoName):
        """
        Function to update the git hash code in the sources.csv by the repo hash code
        """
        self.sources.loc[repoName, "git_commit_hash"] = self[repoName].commit().hexsha
        tag = self.get_tag_of_source(repoName)
        self.sources.loc[repoName, "tag"] = tag

    def setActive(self, repoName):
        """
        Function to set a reposity active
        """
        self[repoName].git.refresh()

    def isSource(self, sourceID):
        if sourceID in self.sources.index:
            self[sourceID].git.refresh()
            return True
        else:
            return False


#%% TqdmProgressPrinter
class TqdmProgressPrinter(git.RemoteProgress):
    known_ops = {
        git.RemoteProgress.COUNTING: "counting objects",
        git.RemoteProgress.COMPRESSING: "compressing objects",
        git.RemoteProgress.WRITING: "writing objects",
        git.RemoteProgress.RECEIVING: "receiving objects",
        git.RemoteProgress.RESOLVING: "resolving stuff",
        git.RemoteProgress.FINDING_SOURCES: "finding sources",
        git.RemoteProgress.CHECKING_OUT: "checking things out",
    }

    def __init__(self):
        super().__init__()
        self.progressbar = None

    def update(self, op_code, cur_count, max_count=None, message=""):
        if op_code & self.BEGIN:
            desc = self.known_ops.get(op_code & self.OP_MASK)
            self.progressbar = tqdm.tqdm(desc=desc, total=max_count)

        self.progressbar.set_postfix_str(message, refresh=False)
        self.progressbar.update(cur_count)

        if op_code & self.END:
            self.progressbar.close()