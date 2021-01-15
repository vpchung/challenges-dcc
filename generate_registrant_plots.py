"""Creates barplots of DREAM challenge registrants."""

from challengeutils import teams
import synapseclient
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.style as style


def query_challenges_table(syn, syn_id):
    """Query a Synapse table for challenge data.

    Assumptions:
        headers follow the naming convention as defined in
        the landscape.
    """

    challenges = syn.tableQuery(
        f"""select challenge, challengeYear, monetaryIncentive,
        containerization, challengeParticipants, challengePreregistrants
        from {syn_id} where challengeYear <> 'TBD' """).asDataFrame().fillna("")
    columns = ["Challenge", "Year", "Registered", "MoneyPrize", "Docker"]
    counts = []
    for _, row in challenges.iterrows():
        reg = teams._get_team_set(syn,  # pylint: disable=W0212
                                  row['challengeParticipants'])
        count = pd.DataFrame(
            [[row['challenge'], row['challengeYear'], len(reg),
              row['monetaryIncentive'], row['containerization']]],
            columns=columns)
        counts.append(count)
    return pd.concat(counts, ignore_index=True)


def plot_table(table_data, yaxis,
               xlab, ylab):
    """Plot a bar plot of the challenge data."""

    participants = sns.catplot(y=yaxis, kind="count",
                               data=table_data, dodge=False)
    participants.set(xlabel=xlab, ylabel=ylab)

    # Per Mike's request: remove grey background and grid lines
    # participants.grid(False)
    # participants.patch.set_facecolor("white")
    # for spine in ['left', 'right', 'top', 'bottom']:
    #     participants.spines[spine].set_color('k')
    plt.show()


def main():
    """Main function.

    Assumptions:
        - Synapse credentials are available in cache.
        - account has access to DREAM Landscape table (syn21645842)
    """

    # Setup initial plot settings
    style.use('seaborn-talk')
    style.use('ggplot')
    plt.rcParams['font.sans-serif'] = 'Lato'
    plt.rcParams['font.family'] = 'sans-serif'
    plt.rcParams['text.color'] = '#909090'
    plt.rcParams['axes.labelcolor'] = '#909090'
    plt.rcParams['xtick.color'] = '#909090'
    plt.rcParams['ytick.color'] = '#909090'
    plt.rcParams['font.size'] = 10

    syn = synapseclient.login(silent=True)

    table = query_challenges_table(syn, "syn21645842")
    plot_table(table, yaxis="Year", xlab="# of Challenges", ylab="Year")
    print(table)


if __name__ == "__main__":
    main()
