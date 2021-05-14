import persistence.postgres as pg
from collections import defaultdict

def getCommentatorListForChannels(comments):
    commentatorsDict = defaultdict(list)
    for comment in comments:
        commentatorsDict[comment['channel_id']].append(comment['creator_channel_id'])
    return commentatorsDict

def openQuery(sqlFile):
    with open(sqlFile, 'r') as file:
        query = file.read()
    file.close()
    return query

postgresHandler = pg.PostgresHandler(write_access=False)

postgresHandler.exec_sql(openQuery('SqlQueries/init.sql'))
all_comments = postgresHandler.retrieve_db_records_from_sql(openQuery('SqlQueries/allComments.sql'))
possibleCombis = postgresHandler.retrieve_db_records_from_sql(openQuery('SqlQueries/crossjoin.sql'))
commentatorsDict = getCommentatorListForChannels(all_comments)

for combi in possibleCombis:
    commentators_a = commentatorsDict.get(combi['a'])
    commentators_b = commentatorsDict.get(combi['b'])
    if commentators_a != None and commentators_b != None:
        intersection = len(list(set(commentators_a) & set(commentators_b)))
        union = len(list(set(commentators_a).union(commentators_b)))
        if(intersection > 0):
            perc = (float(intersection) / float(union))*100.0
            postgresHandler.insert_data(openQuery('SqlQueries/insert.sql') % (combi['a'],combi['b'] , intersection,perc),0.1)