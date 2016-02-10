import FunctionalIoTtypes
import FunctionalProcessing
import Data.List.Split

-- FriendShip Event
data Friendship = Friendship {fuser1::Id, fuser2::Id} deriving (Eq, Ord, Show)
-- Post Event
data Post = Post {ppostId::Id, pUserId::Id, pContent::String, pUserName:: String} deriving (Eq, Ord, Show)
-- Comment Event
-- comment_replied is the id of the comment being replied to (-1 if the tuple is a reply to a post)
-- post_commented is the id of the post being commented (-1 if the tuple is a reply to a comment)
data Comment = Comment {ccommentId::Id, cUserId::Id, cContent::String, cUserName::String, ccommentReplied::Id, cpostReplied::Id}
  deriving (Eq, Ord, Show)
-- Like Event
data Like = Like {lUserId::Id, lCommentId::Id} deriving (Eq, Ord, Show)

type Id = Int

postSource:: String -> Source Post
postSource s = map stringsToPost (map (Data.List.Split.splitOn "|") (lines s))

stringsToPost :: [String] -> Event Post
stringsToPost [ts, postId, userId, content, userName] =
    E (read (parseTimestamp ts)) (Post (read postId) (read userId) content userName)

commentSource:: String -> Source Comment
commentSource s = map stringsToComment (map (Data.List.Split.splitOn "|") (lines s))

stringsToComment :: [String] -> Event Comment
stringsToComment [ts, commentId, userId, comment, userName, commentReplied, postCommented] =
  E (read (parseTimestamp ts)) (Comment (read commentId) (read userId) comment userName (read (parseReplied commentReplied)) (read (parseReplied postCommented)))

-- Empty replied field should be treated as -1
parseReplied :: String -> String
parseReplied "" = "-1"
parseReplied s = s

-- Remove T letter from timestamp to make it valid
parseTimestamp :: String -> String
parseTimestamp ts = filter (/='T') ts

-- readLines :: FilePath -> IO [String]
-- readLines = fmap lines . readFile

-- test
main1 = do contents <- readFile "data/posts.dat"
           putStr $ show $ take 1 $ postSource contents

main2 = do contents <- readFile "data/comments.dat"
           putStr $ show $ take 10 $ commentSource contents

main3 = do contentPost <- readFile "data/posts.dat"
           contentComment <- readFile "data/comments.dat"
           putStrLn $ show $ take 1 $ postSource contentPost
           putStrLn $ show $ take 1 $ commentSource contentComment
