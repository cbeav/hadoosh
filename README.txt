Navigating HDFS from the command line is painful and doesn't feature tab
completion for either paths or commands. The HadooSh interactive shell
offers basic navigation commands to simplify navigating the behemoth.

Currently supported operations:
 - ls
 - cd
 - pwd
 - head [numLines]
 - cat

Planned future actions:
 - mv
 - rm
 - cp

Known bugs:
 - Tab completion fails when using ".."
 - Using numLines with head breaks tab completion

To use HadooSh, just copy the included jar to your Hadoop cluster's
gateway, make sure you've kinit'd if necessary, and run the following:

hadoop jar HadooSh.jar HadooSh




Authors: Chris Beavers and Paul Hobbs
