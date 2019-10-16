**QUICK NOTE**
    It's a test task. Description is available via Google Docs.
    https://docs.google.com/document/d/1TMeXzjVrWCKqYH1jXp62JwZtSZ5qEkfOv-KejQB2g74/edit?usp=sharing
    
    I'm using Lombok, so if you haven't got it installed as 
    an IntelliJ IDEA plugin, highlighting wouldn't work.
    https://projectlombok.org/setup/intellij
    
    You could use logback.xml file to configure logging in your preferable manner.
    Also I've placed an executable JAR with all required dependencies
    into a root of the archive.
    
I was supposed to sort products in such order
    {Price, Name, Condition, State, product ID}.
Lower prices and ids come first, string are sorted in a natural alphabetical order.

I've made two variants of implementation:
`    1. Using Stream API
     2. Using Producer-Consumer pattern and blocking queue for products`
     
I've written an net.ddns.arnautovevgeny.pricelist.AutomationTest, which can generate *.csv files
and then run tests on them.

