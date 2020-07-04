# [Distributed Travel Reservation System](https://www.ics.uci.edu/~cs223/projects/)

```
The goal of the two course projects is to construct a distributed application
in Java that implements a simple travel reservation system. 

You'll implement the project in two stages:

- In Project 1 you'll implement a simple Resource Manager (i.e., a very simple
  database system with a fixed set of tables and operations) that supports
  concurrent transactions with ACID properties.

- In Project 2, you'll implement a Workflow Controller and a Transaction
  Manager that enable distributed transactions across several Resource
  Managers.
```

## Overview

```

         +--------+   +--------+    +--------+                                 
         |        |   |        |    |        |                                 
         | Client |   | Client |    | Client |                                 
         |        |   |        |    |        |                                 
         +--------+   +---+----+    +--------+                                 
                   \      |        /                                           
                    \     |       /                                            
                +----+-----+-----+-+         +-------------------+             
                |                  |---------|                   |             
                |     Workflow     |         |    Transaction    |             
                |    Controller    |         |      Manager      |             
                |                  |         |                   |             
                +---------+----+---+         +---+----+-----+----+             
               /          |     \   \       /   /     |      \                 
              /           |      \   \     /   /      |       \                
             /            |       \   \   /   /       |        \               
            /   /---------|--------\---\-/---/        |         \              
           /   /          |         \   \             |          \             
          /   /           |          \ / \            |           \            
         /   /            |           \---\-----------|---------\  \           
        /   /             |          /     \          |          \  \          
       /   /              |         /       \         |           \  \         
    +-+---+----+        +-+--------+         +--------+-+         ++--+------+ 
    |          |        |          |         |          |         |          | 
    | Resource |        | Resource |         | Resource |         | Resource | 
    | Manager  |        | Manager  |         | Manager  |         | Manager  | 
    |          |        |          |         |          |         |          | 
    +-----+----+        +-----+----+         +-----+----+         +-----+----+ 
          |                   |                    |                    |      
          |                   |                    |                    |      
    +-----+----+        +-----+----+         +-----+----+         +-----+-----+
    |          |        |          |         |          |         |           |
    | Flights  |        |  Hotels  |         |   Cars   |         | Customers |
    |          |        |          |         |          |         |           |
    +----------+        +----------+         +----------+         +-----------+

```

## Shadowing

```

    +----------+             +----------+
    |          |             |          |
    | Resource |   update    | Flights  |
    | Manager  +-------------+ Copy     |
    |          |             |          |
    +-----+----+             +----------+
          |                              
          | commit                       
          |                              
    +-----+----+                         
    |          |                         
    | Flights  |                         
    |          |                         
    +----------+                         

```


## Two Phase Commit

```

        +------------------+                 +-------------------+             
        |                  |   1.commit()    |                   |             
        |     Workflow     |-----------------|    Transaction    |             
        |    Controller    |      6.ok       |      Manager      |             
        |                  |                 |                   |             
        +------------------+                 +---+----+----------+             
                                                /     |                        
                 2.prepare()                   /      |    4.commit()          
                 3.ok           /-------------/       |    5.ok                
                               /                      |                        
                              /                       |                        
                        +----+-----+         +--------+-+                      
                        |          |         |          |                      
                        | Resource |         | Resource |                      
                        | Manager  |         | Manager  |                      
                        |          |         |          |                      
                        +-----+----+         +-----+----+                      

```
