import urllib.request
import re

filename = 'ebayCat.txt'

def getCategoryUrls(url):
    fp = urllib.request.urlopen(url)

    mybytes = fp.read()
    contents = mybytes.decode("utf8")
     
    fp.close()

    regex = re.compile("<td colspan=\"5\"><a href=\"(.*?)\"><i>(.*?)<\/i><\/a><\/td>",re.IGNORECASE)
    match = regex.findall(contents)

    categoriesUrls = []

    for x in match:
        categoriesUrls.append([x[0][(x[0].rfind("<a href=\"") + 9):len(x[0])],x[1][x[1].find("See all ") + 8:x[1].rfind(" categories...")]])
    
    return categoriesUrls

def getCategories(categoryName, url):

    fp = urllib.request.urlopen(url)

    mybytes = fp.read()
    contents = mybytes.decode("utf8")
    
    fp.close()
    
    #print(contents)

    #regex = re.compile("<a href=\"(.*?)\">(.*?)<\/a>[\s]+\(([\d]+)\)<\/td>",re.IGNORECASE)
    #regex = re.compile("<td colspan=\"([\d]+)\">(.|[\s])*?<a href=\"(.*?)\">(.*?)<\/a>(.|[\s])*?\(([\d]+)\)<\/td>",re.IGNORECASE)
    regex = re.compile("<td colspan=\"([\d]+)\">[\s]+(<b>[\s]+)?<a href=\"(.*?)\">(.*?)<\/a>[\s]+(<\/b>[\s]+)?\(([\d]+)\)<\/td>",re.IGNORECASE)
    match = regex.findall(contents)

    f = open(filename, 'ab')

    maxLevel = 4
    catNames = []
    for i in range(maxLevel):
        catNames.append("")

    previousLevel = 100
    line = ""

    for x in match:
        tempLevel = 6 - int(x[0])
        if(tempLevel < maxLevel):
            level = tempLevel

            if(previousLevel >= level):
                f.write(line.encode("utf8"))
            
            catNames[level] = x[3]
            quantity = x[5]
            line = categoryName + "\t"
            for i in range(maxLevel):
                if(i <= level):
                    line += catNames[i]
                # line += "\t"
                line += "\t"
            line += quantity + "\n"
            previousLevel = level

    f.write(line.encode("utf8"))

    f.close()

f = open(filename, 'w')
f.write("")
f.close()

categories = getCategoryUrls("http://listings.ebay.com/_W0QQfclZ1QQsocmdZListingCategoryList")

for category in categories:
    print(category)
    getCategories(category[1],category[0])

#getCategories("http://computers.listings.ebay.com/_W0QQfclZ1QQsocmdZListingCategoryOverview")
#getCategories("collectable","http://collectibles.listings.ebay.com/_W0QQfclZ1QQsocmdZListingCategoryOverview")
    
