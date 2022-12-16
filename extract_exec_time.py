"""Estimate Docker Submission Duration

This script will iterate through a SubmissionView and return an
estimation of each submission's model run in seconds.

Assumptions:
    - Log files were produced by SynapseWorkflowOrchestator, that is,
      logs are expected to be formatted as TOIL logs.
    - Synapse credentials are provided in .synapseConfig
"""
import os
import re
import argparse
import zipfile
from datetime import datetime

import synapseclient


def get_args():
    """Set up command-line interface and get arguments."""
    parser = argparse.ArgumentParser(
        description="Extract estimated Docker run exec time from log files.")
    parser.add_argument("-s", "--submission_view_id",
                        type=str, required=True,
                        help="Synapse ID of SubmissionView")
    parser.add_argument("-o", "--output_file",
                        type=str, default="results.csv",
                        help="Filename for output CSV file")
    parser.add_argument("--dryrun", action="store_true")
    return parser.parse_args()


def get_submissions(syn, view_id):
    """
    Return df of `id` and `submitterid` of ACCEPTED submissions.

    Assumptions:
        - Valid and scored submissions have `status` of ACCEPTED.
    """
    query = (
        "SELECT id, submitterid "
        f"FROM {view_id} "
        "WHERE status = 'ACCEPTED'"
    )
    return syn.tableQuery(query).asDataFrame()


def get_team_name(syn, uid):
    """Return team name or username of given ID."""
    name = ""
    try:
        name = syn.getTeam(uid).get('name')
    except synapseclient.core.exceptions.SynapseHTTPError:
        name = syn.getUserProfile(uid).get('userName')
    return name


def find_log_file(syn, submission):
    """Return Synapse ID and filename of log file."""
    folder_id = [
        x.get('value')
        for x in submission.get('annotations').get('stringAnnos')
        if x.get('key').endswith("SubmissionFolder")
    ][0]
    filename = f"{submission.id}_logs"
    file_id = syn.findEntityId(f"{filename}.zip", folder_id)
    return file_id, filename


def unzip_file(f, path="."):
    """Unzip file."""
    if zipfile.is_zipfile(f):
        with zipfile.ZipFile(f) as zip_ref:
            zip_ref.extractall(path=path)
    else:
        print(f"{f} is not a zip file.")


def calc_exec_time(log_file):
    """
    Find time difference in sec between when run_docker job starts
    and ends.

    Important!
        - Returned time includes time it takes to `docker pull`.
    """
    with open(log_file) as f:
        log = f.read()
        try:
            start_time = re.search(
                r"STDERR: ([\d\-T:]+?\.\d{6})\d+?Z.*?Issued job.*?run_docker\.cwl",
                log).group(1)
            start_time = datetime.fromisoformat(start_time)
            end_time = re.search(
                r"STDERR: ([\d\-T:]+?\.\d{6})\d+?Z.*?Job ended.*?run_docker\.cwl",
                log).group(1)
            end_time = datetime.fromisoformat(end_time)
        except AttributeError:
            return "run_docker step not found"
        else:
            exec_time = end_time - start_time
            return exec_time.total_seconds()


def main():
    """Main function."""
    args = get_args()
    syn = synapseclient.Synapse()
    try:
        syn.login(silent=True)
    except synapseclient.core.exceptions.SynapseNoCredentialsError:
        syn.login(authToken=os.getenv('authtoken'),
                  silent=True)

    submissions_df = get_submissions(syn, args.submission_view_id)
    submissions_df["team_name"] = ""
    submissions_df["exec_time(s)"] = ""
    for _, row in submissions_df.iterrows():
        # Find team/username based on submitterid (for easier comprehension).
        submissions_df.at[_, "team_name"] = get_team_name(
            syn, row['submitterid'])

        # Download zipped log file.
        sub_id = row['id']
        submission = syn.getSubmissionStatus(sub_id)
        log_id, filename = find_log_file(syn, submission)
        syn.get(log_id, downloadLocation=".")

        # Unzip file, then extract time duration from logs.
        unzip_file(f"{filename}.zip")
        submissions_df.at[_, "exec_time(s)"] = calc_exec_time(
            f"{filename}.txt")

        # Remove zip + txt file, since they're no longer needed.
        for ext in [".zip", ".txt"]:
            os.remove(filename + ext)

        # For dryruns, only perform one iteration.
        if args.dryrun:
            break
    submissions_df.to_csv(args.output_file, index=False)


if __name__ == "__main__":
    main()
