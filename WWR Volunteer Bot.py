import pandas, pytz, discord, dotenv, os
from datetime import datetime, timezone, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from discord.ext import commands
from dotenv import load_dotenv

TargetSheet = "https://docs.google.com/spreadsheets/d/1H19xsapwJxxqxcJU2EbH82zsBqltpYVaQ1I7Kj8Pbp0/export?format=csv&id=1H19xsapwJxxqxcJU2EbH82zsBqltpYVaQ1I7Kj8Pbp0&gid=0" #"https://docs.google.com/spreadsheets/d/1scFXT8bjyiqkFjihpbj8n5CpT_5IItA3qZYjP3lkwaY/export?format=csv&id=1scFXT8bjyiqkFjihpbj8n5CpT_5IItA3qZYjP3lkwaY&gid=0"

StartingRowIdentifier = "Date ET"
UTCColumnIdentifier = "Time (UTC)"
CommentatorIdentifier = "Commentators"
TrackerIdentifier = "Trackers"

TestCommentatorRoleID = "<@&1411103550595137536>" # role id for comms

TestTrackerRoleID = "<@&1411103607075635220>" # role id for trackers

TestChannelID = 1409607946786046055 # channel id for #volunteer-chat

load_dotenv()
token = os.getenv("BotToken")
WWRVolunteersChatChannelID = int(os.getenv("WWRVolunteerChatChannelID"))
CommentatorRoleID = os.getenv("CommentatorRoleID")
TrackerRoleID = os.getenv("TrackerRoleID")

AdvancePingTimeframe = 2 # days before race that it should ping

StoredMatchesFile = os.path.join(os.getcwd(), "StoredMatches.txt")

with open(StoredMatchesFile, "r", encoding = "utf-8") as file:
    PingedMatches = file.read().splitlines() # holds all the uuids for matches that already have been pinged. used to check a current row against this list, so that we don't reping every hour. 
    file.close()

bot = commands.Bot(command_prefix="!", intents=discord.Intents.default())

Scheduler = AsyncIOScheduler()

async def CheckSheet():

    global KeyColumnIndexes

    RawRaceList = pandas.read_csv(TargetSheet)

    NewHeaderRowIndex = None

    for row in RawRaceList.itertuples():
        if NewHeaderRowIndex is None:
            for col in range(len(row)):
                if isinstance(row[col], str) and row[col].strip() == StartingRowIdentifier:
                    NewHeaderRowIndex = row.Index
                    break
        else:
            break

    CleanedRaceList = RawRaceList.iloc[NewHeaderRowIndex:]
    
    CleanedRaceList = CleanedRaceList[1:]

    CleanedRaceList.columns = RawRaceList.iloc[NewHeaderRowIndex].str.strip()

    CleanedRaceList = CleanedRaceList.replace(r"\n", " ", regex = True)

    DateColumnIndex = CleanedRaceList.columns.get_loc(StartingRowIdentifier) + 1

    UTCColumnIndex = CleanedRaceList.columns.get_loc(UTCColumnIdentifier) + 1

    CommentatorIndex = CleanedRaceList.columns.get_loc(CleanedRaceList.columns[CleanedRaceList.columns.str.contains(CommentatorIdentifier, regex=False)][0]) + 1

    TrackerIndex = CleanedRaceList.columns.get_loc(CleanedRaceList.columns[CleanedRaceList.columns.str.contains(TrackerIdentifier, regex=False)][0]) + 1

    KeyColumnIndexes = [DateColumnIndex, UTCColumnIndex, CommentatorIndex, TrackerIndex]

    CurrentTime = datetime.now(timezone.utc)

    MaxPingTime = CurrentTime + timedelta(days = AdvancePingTimeframe) 

    for row in CleanedRaceList.itertuples():
        try:
            UTCFullDateTime = pytz.utc.localize(datetime.strptime(f"{str(row[DateColumnIndex])} {str(row[UTCColumnIndex])} {datetime.now().year}", "%b %d %H:%M %Y"))
            if UTCFullDateTime <= MaxPingTime:
                DiscordMessage, UUID = await CreateDiscordMessage(row, UTCFullDateTime)
                if DiscordMessage != "":
                    await TransmitMessage(DiscordMessage)
                    with open(StoredMatchesFile, "a", encoding = "utf-8") as file:
                        file.write(f"{UUID}\n")
        except:
            pass
                
async def CreateDiscordMessage(row, UTCtimestamp):
    global KeyColumnIndexes, TimeStamp
    UUID = str(UTCtimestamp) + str(row.Race) + str(row.Round)
    FullMessagetoSend = ""
    if UUID not in PingedMatches:
        if "Qual" in row.Race + row.Round:
            VolunteerMinimum = 4
        else:
            VolunteerMinimum = 2
        PingedMatches.append(UUID)
        MatchName = f"{str(row.Race)} {str(row.Round)}"
        TimeStamp = int(datetime.timestamp(UTCtimestamp))
        CommentatorText = row[KeyColumnIndexes[2]]
        Commentators, Trackers = [], []
        if not pandas.isna(CommentatorText):
            Commentators = CommentatorText.replace(" ", "").split(",")
        TrackerText = row[KeyColumnIndexes[3]]
        if not pandas.isna(TrackerText):
            Trackers = TrackerText.replace(" ", "").split(",")
        DoubleDuty = list(set(Commentators) & set(Trackers))
        RequiredCommentators = VolunteerMinimum - len(Commentators)
        RequiredTrackers = VolunteerMinimum - len(Trackers)
        while len(DoubleDuty) > 0:
            if len(Commentators) > len(Trackers):
                Commentators.remove(DoubleDuty[0])
            elif len(Commentators) <= len(Trackers): # less people sign up to commentate than track, so we by default just assume the commentator will drop out of tracking
                Trackers.remove(DoubleDuty[0])
            RequiredCommentators = VolunteerMinimum - len(Commentators)
            RequiredTrackers = VolunteerMinimum - len(Trackers)
            DoubleDuty = list(set(Commentators) & set(Trackers))
        if RequiredCommentators <= 0 and RequiredTrackers <= 0:
            return FullMessagetoSend
        elif RequiredCommentators > 0 and RequiredTrackers > 0:
            FullMessagetoSend += f"{TestCommentatorRoleID} {TestTrackerRoleID} {MatchName} is scheduled for <t:{TimeStamp}:f>, and we need {RequiredCommentators} commentator(s) and {RequiredTrackers} tracker(s).\nPlease sign up using this spreadsheet https://zsr.link/twwrvolunteer"
        elif RequiredCommentators > 0 and RequiredTrackers < 0:
            FullMessagetoSend += f"{TestCommentatorRoleID} {MatchName} is scheduled for <t:{TimeStamp}:f>, and we need {RequiredCommentators} commentator(s).\nPlease sign up using this spreadsheet https://zsr.link/twwrvolunteer"
        elif RequiredCommentators < 0 and RequiredTrackers > 0:
            FullMessagetoSend += f"{TestTrackerRoleID} {MatchName} is scheduled for <t:{TimeStamp}:f>, and we need {RequiredTrackers} trackers.\nPlease sign up using this spreadsheet https://zsr.link/twwrvolunteer"
        return FullMessagetoSend, UUID

@bot.event
async def on_ready():
    Scheduler.add_job(CheckSheet, "interval", seconds = 10)
    Scheduler.start()

async def TransmitMessage(DiscordMessage):
    channel = bot.get_channel(TestChannelID)
    await channel.send(content = DiscordMessage, embed = None)

bot.run(token)