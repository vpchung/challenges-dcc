"""Prints out metrics for Ryan Luce's Synapse reports.

Required:
    -c/--challenge_ids  One or more Synapse project IDs

Optional:
    -s/--start_date     Start date for submissions tracking
    -e/--end_date       End date for submissions tracking
"""

import re
import argparse
from datetime import datetime

import synapseclient
from challengeutils import utils
from challengeutils import teams


def get_args():
    """Set up command-line interface and get arguments."""

    parser = argparse.ArgumentParser(
        description=("Get registrant and submission numbers for "
                     "Synapse reports.")
    )

    parser.add_argument("-c", "--challenge_ids",
                        type=str, nargs="+", required=True,
                        help="List of challenge Synapse IDs")
    parser.add_argument("-s", "--start_date",
                        type=str, default=None,
                        help=("Start datetime for submissions tracking in "
                              "YYYY-MM-DDThh:mm:ss format"))
    parser.add_argument("-e", "--end_date",
                        type=str, default=None,
                        help=("End datetime for submissions tracking in "
                              "YYYY-MM-DDThh:mm:ss format)"))
    return parser.parse_args()


def get_challenge_info(syn, challenge):
    """Get information about a given challenge.

    Returns:
        c_id: challenge ID (which is different from Synapse ID)
        name: challenge name
        team: participant team ID for challenge
        eval_ids: list of evaluation IDs for challenge
    """

    name = syn.restGET(f"/entity/{challenge}").get("name")

    challenge_obj = syn.restGET(f"/entity/{challenge}/challenge")
    c_id = challenge_obj.get("id")
    team = challenge_obj.get("participantTeamId")

    # Filter out IDs of evaluations for writeups and tests.
    eval_obj = syn.restGET(f"/entity/{challenge}/evaluation").get("results")
    eval_ids = [e.get("id") for e in eval_obj
                if not re.search(r"test|write-up|uw", e.get("name"), re.I)]

    return c_id, name, team, eval_ids


def convert_to_epoch(dt):  # pylint: disable-msg=C0103
    """Convert given datetime to Epoch timestamp in milliseconds."""

    return int(datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S").timestamp() * 1000)


def count_submissions(syn, evaluations, start=None, end=None):
    """Tally up all valid submissions from given evaluations.

    Returns:
        total: total number of submissions from challenge queues
    """

    if start:
        start = convert_to_epoch(start)
    if end:
        end = convert_to_epoch(end)

    total = 0
    for eval_id in evaluations:
        query = f"SELECT * FROM evaluation_{eval_id} WHERE status == 'ACCEPTED'"
        if start:
            query += f" AND createdOn >= {start}"
        if end:
            query += f" AND createdOn <= {end}"
        total += sum(1 for sub in utils.evaluation_queue_query(
            syn, query, limit=13))
    return total


def print_report(syn, args):
    """Print report."""

    unique_users = set()
    total_submissions = 0
    for challenge in args.challenge_ids:
        c_id, name, team, eval_ids = get_challenge_info(syn, challenge)
        participants = {user.get('ownerId')
                        for user in teams._get_team_set(syn, team)}  # pylint: disable=W0212

        # Keep track of unique users across all challenges.
        unique_users = unique_users.union(participants)

        # Count number of submissions for challenge, then add to total.
        submissions = count_submissions(syn, eval_ids,
                                        args.start_date,
                                        args.end_date)
        total_submissions += submissions

        print("\t".join([c_id, name, team,
                         str(len(participants)),
                         str(submissions)]))

    print("=" * 20)
    print("Total registrants for challenges:", len(unique_users))
    print("Total submissions for challenges:", total_submissions)


def main():
    """Main function.

    Assumptions:
        Synapse credentials are available in cache.
    """

    syn = synapseclient.login(silent=True)
    args = get_args()
    print_report(syn, args)


if __name__ == "__main__":
    main()
