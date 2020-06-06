# Group 1
# Elsy Fernandes 1001602253
# Maria Lancy Devadoss 1001639262
import pandas
from pymongo import MongoClient
from dateutil import parser

client = MongoClient('mongodb://localhost:27017/')  # connecting to local mongodb
db = client.WORLD_CUP_SOCCER # creating WORLD_CUP database
TEAM_SCORES = db.TEAM_SCORES  # creating TEAM_SCORES collection
PLAYER_DATA = db.PLAYER_DATA  # creating PLAYER_DATA collection

# reading all the given data files in the csv format
team_info = pandas.read_csv("/Users/el/Desktop/DB2/db2 project2/DB2/Team.csv")
teams = team_info.TeamID  # distinct TeamID names
players_info = pandas.read_csv("/Users/el/Desktop/DB2/db2 project2/DB2/Players.csv")
players = players_info.FIFAPopularName  # distinct player names
stadium_info = pandas.read_csv("/Users/el/Desktop/DB2/db2 project2/DB2/Stadium.csv")
game_info = pandas.read_csv("/Users/el/Desktop/DB2/db2 project2/DB2/Game.csv")
goal_info = pandas.read_csv("/Users/el/Desktop/DB2/db2 project2/DB2/Goals.csv")
Starting_Lineups_data = pandas.read_csv("/Users/el/Desktop/DB2/db2 project2/DB2/Starting_Lineups.csv")

TEAM_SCORES.delete_many({})  # removing data if any
PLAYER_DATA.delete_many({})  # removing data if any

# fetching and inserting data into TEAM_SCORES collection
for i in teams:  # iterating through each team
    result_doc = {}  # initialising dictionary result_doc for each team
    team_details = team_info[team_info['TeamID'] == i]
    result_doc['Team'] = team_details.Team.values[0] # fetching Team Name
    game_details1 = game_info[game_info['TeamID1'] == i]
    game_details2 = game_info[game_info['TeamID2'] == i]
    game_details = game_details1.append(game_details2)
    schedule_results = game_details[['MatchDate', 'SID', 'TeamID1', 'TeamID2', 'Team1_Score', 'Team2_Score']]  # seggregating needed columns
    games_list = []  # list for adding each game details of the team
    for index, row in schedule_results.iterrows():  # iterating through each game to get the game details of the team
        doc = {}  # result_doc for each game
        doc['MatchDate'] = parser.parse(row['MatchDate'])  # fetching Match Date
        # Fetching Stadium Name and City
        stadiumInfo_collection = stadium_info[stadium_info['SID'] == row['SID']]
        doc['Stadium City'] = stadiumInfo_collection.SCity.values[0]
        doc['Stadium Name'] = stadiumInfo_collection.SName.values[0]
        # Fetching opposing team names based on TeamID
        if(row['TeamID1'] == i):
            TeamInfo_collection = team_info[team_info['TeamID'] == row['TeamID1']]
            doc['TeamName'] = TeamInfo_collection.Team.values[0]
            doc['TeamScore'] = row['Team1_Score']  # inserting Team1_Score
            Opposition_collection = team_info[team_info['TeamID'] == row['TeamID2']]
            doc['OpposingTeam'] = Opposition_collection.Team.values[0]
            doc['OpposingScore'] = row['Team2_Score']  # inserting Team2_Score
        elif(row['TeamID2'] == i):
            TeamInfo_collection = team_info[team_info['TeamID'] == row['TeamID2']]
            doc['TeamName'] = TeamInfo_collection.Team.values[0]
            doc['TeamScore'] = row['Team2_Score']  # inserting Team1_Score
            Opposition_collection = team_info[team_info['TeamID'] == row['TeamID1']]
            doc['OpposingTeam'] = Opposition_collection.Team.values[0]
            doc['OpposingScore'] = row['Team1_Score']  # inserting Team2_Score
        games_list.append(doc)  # adding all details of particular game into the list of games collection
    result_doc['Game_scores'] = games_list
    # inserting game list and finally all the data into TEAM_SCORES collection
    team_score_insert = TEAM_SCORES.insert_one(result_doc)

# fetching and inserting data into PLAYER_DATA collection
for i in players:  # iterating through each player
    result_doc = {}  # initialising dictionary result_doc for each player
    players_details = players_info[
        players_info['FIFAPopularName'] == i]  # fetching all the rows from the dataframe for corresponding player
    player_specific_info = players_details[
        ['Team', 'TeamID', 'PlayerID', 'Position', 'FIFAPopularName']]  # seggregating needed columns
    result_doc['Player Name'] = players_details.FIFAPopularName.values[0] # fetching Player Name
    Team_collection = team_info[team_info['TeamID'] == players_details.TeamID.values[0]]
    result_doc['Team'] = Team_collection.Team.values[0] # fetching Team Name of the player
    pno=int(players_details.PlayerID.values[0])
    result_doc['PNumber'] = pno # fetching player Number
    result_doc['Position'] = players_details.Position.values[0] # Fetching player position
    # Fetching GameID where the player has started the Game using TeamID and playerID
    game_details = Starting_Lineups_data[Starting_Lineups_data['TeamID'] == players_details.TeamID.values[0]]
    game_details = game_details[game_details['PlayerID'] == players_details.PlayerID.values[0]]
    # Fetching details of that GameID alone from Game table
    if(game_details.empty):
        games_list = []
    else:
        game_details = game_info[game_info['GameID'] == game_details.GameID.values[0]]
        # print(game_details)
        schedule_results = game_details[['MatchDate', 'SID', 'TeamID1', 'TeamID2']]  # seggregating needed columns from df
        games_list = []  # list for adding each game which player started
        for index, row in schedule_results.iterrows():  # iterating through each game to get the details of game player started
            doc = {}  # result_doc for each game
            doc['MatchDate'] = parser.parse(row['MatchDate'])  # Fetching date of match
            # Fetching Stadium Name and city
            stadiumInfo_collection = stadium_info[stadium_info['SID'] == row['SID']]
            doc['Stadium City'] = stadiumInfo_collection.SCity.values[0]
            doc['Stadium Namx`e'] = stadiumInfo_collection.SName.values[0]
            # Fetching opposing team name based on TeamID
            if(row['TeamID1'] == players_details.TeamID.values[0]):
                Opposition_collection = team_info[team_info['TeamID'] == row['TeamID2']]
                doc['OpposingTeam'] = Opposition_collection.Team.values[0]
            elif(row['TeamID1'] == players_details.TeamID.values[0]):
                Opposition_collection = team_info[team_info['TeamID'] == row['TeamID1']]
                doc['OpposingTeam'] = Opposition_collection.Team.values[0]
            games_list.append(doc)  # adding all details of player started games into the list game_list
    result_doc['Player_Game_Started'] = games_list

    # fetching all goals details belongs to the particular player based on PlayerID and TeamID
    goal_details = goal_info[goal_info['TeamID'] == players_details.TeamID.values[0]]
    goal_details = goal_details[goal_details['PlayerID'] == players_details.PlayerID.values[0]]
    goal_specific_info = goal_details[['GameID', 'TeamID', 'PlayerID', 'Time','Penalty']]  # seggregating needed columns
    goals_list = []  # list for adding each goal
    for index, row in goal_specific_info.iterrows():  # iterating through each goal data to get the player goal details
        doc = {}  # result_doc for each player
        # Fetching Goal Type.If Penalty is Y Goal type is Penalty else its Regular
        if (row['Penalty'] == 'N'):
            doc['Goal Type'] = 'Regular'
        else:
            doc['Goal Type'] = 'Penalty'
        doc['Time'] = row['Time'] # Fetching Time of goal
        MatchDate_collection = game_info[game_info['GameID'] == row['GameID']]
        doc['MatchDate'] = MatchDate_collection.MatchDate.values[0] # Fetching Match date
        stadiumInfo_collection = stadium_info[stadium_info['SID'] == MatchDate_collection.SID.values[0]]
        doc['Stadium City'] = stadiumInfo_collection.SCity.values[0] # Fetching Stadium name and city
        doc['Stadium Name'] = stadiumInfo_collection.SName.values[0]
        # Fetching opposing Team Name
        if(MatchDate_collection.TeamID1.values[0]==players_details.TeamID.values[0]):
            Opposition_collection = team_info[team_info['TeamID'] == MatchDate_collection.TeamID2.values[0]]
            doc['OpposingTeam'] = Opposition_collection.Team.values[0]
        elif(MatchDate_collection.TeamID2.values[0]==players_details.TeamID.values[0]):
            Opposition_collection = team_info[team_info['TeamID'] == MatchDate_collection.TeamID1.values[0]]
            doc['OpposingTeam'] = Opposition_collection.Team.values[0]
        goals_list.append(doc)  # adding all details of goals into the list of goals
    result_doc['Player_Goals_collection'] = goals_list  # adding the goal list into goals collection of each player
    # inserting goals list and finally all the data into PLAYER_DATA collection
    player_data_insert = PLAYER_DATA.insert_one(result_doc)









