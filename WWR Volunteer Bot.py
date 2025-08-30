import pandas, pytz, discord, dotenv, os, sys, traceback, re
from datetime import datetime, timezone, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv

StartingRowIdentifier = "Date ET"
UTCColumnIdentifier = "Time (UTC)"
CommentatorIdentifier = "Commentators"
TrackerIdentifier = "Trackers"

load_dotenv()

CurrentBot = "Main" # Main or Beta, determines the bot input params

match CurrentBot:
    case "Main":
        token = os.getenv("BotToken")
        WWRVolunteersChatChannelID = int(os.getenv("WWRVolunteerChatChannelID")) # channel id for #volunteer-chat
        CommentatorRoleID = os.getenv("CommentatorRoleID") # role id for comms
        TrackerRoleID = os.getenv("TrackerRoleID") # role id for trackers
        TargetSheet = os.getenv("TargetSheet") # sheet to go for
    case "Beta":
        token = os.getenv("BetaBotToken")
        TargetSheet = "https://docs.google.com/spreadsheets/d/1H19xsapwJxxqxcJU2EbH82zsBqltpYVaQ1I7Kj8Pbp0/export?format=csv&id=1H19xsapwJxxqxcJU2EbH82zsBqltpYVaQ1I7Kj8Pbp0&gid=0"
        CommentatorRoleID = "<@&1411103550595137536>" 
        TrackerRoleID = "<@&1411103607075635220>"
        WWRVolunteersChatChannelID = 1409607946786046055
    case _:
        pass

ErrorLogChannelID = int(os.getenv("ErrorLogChannelID")) # error logs get put here

AdvancePingTimeframe = 48 # hours before race that it should ping

StoredMessagesFile = os.path.join(os.path.dirname(os.path.abspath(__file__)), "StoredMessageIDs.txt")

FullRaceList = [] # holds the ScheduledRace objects

class ScheduledRace:
    def __init__(self, MessageInfo):
        self.UUID = MessageInfo[0]
        self.name = MessageInfo[1]
        self.time = MessageInfo[2]
        self.neededcomms = MessageInfo[3]
        if isinstance(self.neededcomms, str):
            self.neededcomms = int(self.neededcomms)
        self.neededtrackers = MessageInfo[4]
        if isinstance(self.neededtrackers, str):
            self.neededtrackers = int(self.neededtrackers)
        try:
            self.messageID = MessageInfo[5]
        except:
            self.messageID = None

    async def StoreMessage(self):
        with open(StoredMessagesFile, "a", encoding = "utf-8") as file:
            file.write(f"[{self.UUID}, {self.name}, {self.time}, {self.neededcomms}, {self.neededtrackers}, {self.messageID}]\n")

with open(StoredMessagesFile, "r", encoding = "utf-8") as file:
    MessageInfo = file.read().splitlines() # holds all the message ids for previously sent messages in case we need to change them later
    file.close()

for race in MessageInfo:
    race = race[1:-1]
    FullRaceList.append(ScheduledRace(race.split(", ")))

PingedMatches = [race.UUID for race in FullRaceList]

bot = discord.Client(intents=discord.Intents.default())

Scheduler = AsyncIOScheduler()

async def CheckSheet():

    try:
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
        MaxPingTime = CurrentTime + timedelta(hours = AdvancePingTimeframe) 

        for row in CleanedRaceList.itertuples():
            UTCFullDateTime = pytz.utc.localize(datetime.strptime(f"{str(row[DateColumnIndex])} {str(row[UTCColumnIndex])} {datetime.now().year}", "%b %d %H:%M %Y"))
            if UTCFullDateTime <= MaxPingTime:
                DiscordMessage, UUID, Race = await CreateDiscordMessage(row, UTCFullDateTime)
                if DiscordMessage != "" and Race != None:
                    MessageID = await TransmitMessage(DiscordMessage)
                    Race.messageID = MessageID.id
                    await Race.StoreMessage()
                    FullRaceList.append(Race)

    except Exception as error:
        ErrorMessage = ""
        exc_type, exc_obj, tb = sys.exc_info()
        filename = tb.tb_frame.f_code.co_filename
        linenum = tb.tb_lineno
        line_text = traceback.extract_tb(tb)[-1].line
        ErrorMessage += f"Error Type: {exc_type.__name__}\n"
        ErrorMessage += f"Error Message: {error}\n"
        ErrorMessage += f"File: {filename}\n"
        ErrorMessage += f"Line #: {linenum}\n"
        ErrorMessage += f"Code Line: {line_text}\n"
        await TransmitError(ErrorMessage)
                
async def CreateDiscordMessage(row, UTCtimestamp):
    global KeyColumnIndexes, TimeStamp
    UUID = str(UTCtimestamp) + str(row.Race) + str(row.Round)
    FullMessagetoSend, RaceObject = "", None
    if UUID not in PingedMatches:
        PingedMatches.append(UUID)
        MatchName = f"{str(row.Race)} {str(row.Round)}"
        TimeStamp = int(datetime.timestamp(UTCtimestamp))
        RequiredCommentators, RequiredTrackers = await DetermineVolunteerReqs(row)
        if RequiredCommentators <= 0 and RequiredTrackers <= 0:
            return FullMessagetoSend, UUID, RaceObject
        elif RequiredCommentators > 0 and RequiredTrackers > 0:
            FullMessagetoSend += f"{CommentatorRoleID} {TrackerRoleID} {MatchName} is scheduled for <t:{TimeStamp}:f>, and we need {RequiredCommentators} commentator(s) and {RequiredTrackers} tracker(s).\nPlease sign up using this spreadsheet: <https://zsr.link/twwrvolunteer>"
        elif RequiredCommentators > 0 and RequiredTrackers <= 0:
            FullMessagetoSend += f"{CommentatorRoleID} {MatchName} is scheduled for <t:{TimeStamp}:f>, and we need {RequiredCommentators} commentator(s).\nPlease sign up using this spreadsheet: <https://zsr.link/twwrvolunteer>"
        elif RequiredCommentators <= 0 and RequiredTrackers > 0:
            FullMessagetoSend += f"{TrackerRoleID} {MatchName} is scheduled for <t:{TimeStamp}:f>, and we need {RequiredTrackers} tracker(s).\nPlease sign up using this spreadsheet: <https://zsr.link/twwrvolunteer>"
        RaceObject = ScheduledRace([UUID, MatchName, TimeStamp, RequiredCommentators, RequiredTrackers])
    else:
        for race in FullRaceList:
            if race.UUID == UUID:
                RequiredCommentators, RequiredTrackers = await DetermineVolunteerReqs(row)
                if race.neededcomms != RequiredCommentators and max(race.neededcomms, RequiredCommentators) > 0:
                    await EditMessage(race, "commentator", RequiredCommentators)
                    race.neededcomms = RequiredCommentators
                    await race.StoreMessage()
                if race.neededtrackers != RequiredTrackers and max(race.neededtrackers, RequiredTrackers) > 0:
                    await EditMessage(race, "tracker", RequiredTrackers)
                    race.neededtrackers = RequiredTrackers
                    await race.StoreMessage()
                break

    return FullMessagetoSend, UUID, RaceObject

async def DetermineVolunteerReqs(row):
    if "Qual" in row.Race + row.Round:
            VolunteerMinimum = 4
    else:
        VolunteerMinimum = 2
    CommentatorText = row[KeyColumnIndexes[2]]
    Commentators, Trackers = [], []
    if not pandas.isna(CommentatorText):
        Commentators = CommentatorText.replace(" ", "").split(",")
    TrackerText = row[KeyColumnIndexes[3]]
    if not pandas.isna(TrackerText):
        Trackers = TrackerText.replace(" ", "").split(",")
    DoubleDuty = list(set(Commentators) & set(Trackers))
    RequiredCommentators = 2 - len(Commentators) # we really only need 2 commentators at most
    RequiredTrackers = VolunteerMinimum - len(Trackers)
    while len(DoubleDuty) > 0:
        if len(Commentators) > len(Trackers):
            Commentators.remove(DoubleDuty[0])
        elif len(Commentators) <= len(Trackers): # less people sign up to commentate than track, so we by default just assume the commentator will drop out of tracking
            Trackers.remove(DoubleDuty[0])
        RequiredCommentators = 2 - len(Commentators)
        RequiredTrackers = VolunteerMinimum - len(Trackers)
        DoubleDuty = list(set(Commentators) & set(Trackers))
    return RequiredCommentators, RequiredTrackers

@bot.event
async def on_ready():
    Scheduler.add_job(CheckSheet, "interval", hours = 1)
    Scheduler.start()

async def EditMessage(Race: ScheduledRace, TargetText, NewPersonCount = 0):
    DiscordMessageID = Race.messageID
    TargetMessage = await bot.get_channel(WWRVolunteersChatChannelID).fetch_message(DiscordMessageID)
    OriginalMessage = TargetMessage.content
    NewMessage = await ReplaceOldMessage(OriginalMessage, TargetText, NewPersonCount)
    await TargetMessage.edit(content = NewMessage)
    await UpdateStoredMessageFile(Race)

async def ReplaceOldMessage(OriginalMessage, PersonType, NewPersonCount):
    RegexPattern = rf"(?:~~(\d+)~~\s*)?(\d+)\s+({PersonType}?)"

    def ReplaceVolunteerNum(match):
        StruckthroughNumber = match.group(1)       # old struck number (if exists)
        CurrentNum = int(match.group(2))     # current visible number
        TargetWord = match.group(3)         # tracker(s) or commentator(s)
        
        return f"~~{CurrentNum}~~ {NewPersonCount} {TargetWord}"

    NewMessage = re.sub(RegexPattern, ReplaceVolunteerNum, OriginalMessage, flags=re.IGNORECASE)

    return NewMessage

async def UpdateStoredMessageFile(Race: ScheduledRace):
    with open(StoredMessagesFile, "r+", encoding = "utf-8") as file:
        rows = file.read().splitlines()
        for row in rows:
            if row.split(", ")[5] == f"{Race.messageID}]":
                rows.remove(row)
    with open(StoredMessagesFile, "w", encoding = "utf-8") as file:
        file.write("\n".join(rows))
        file.write("\n")

async def TransmitMessage(DiscordMessage):
    channel = bot.get_channel(WWRVolunteersChatChannelID)
    message = await channel.send(content = DiscordMessage)
    return message

async def TransmitError(ErrorMessage):
    channel = bot.get_channel(ErrorLogChannelID)
    await channel.send(content = ErrorMessage)

bot.run(token)