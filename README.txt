Ever wanted to grep a file on HDFS and store the results locally?
Now you can, with one simple command in HadooSh:

cbeavers > cd books/
books > cat Ulysses-part-00000 | grep Mulligan >l localOut.txt

HadooSh is an interactive shell for HDFS built on top of JLine to offer
tab completion of commands and paths under Hadoop. It supports piping
to local system commands, and both local and remote output. There are
many bugs to be found, so please play nicely with it.

Currently supported operations:
 - ls [dir]
 - cd [dir]
 - pwd
 - head [numLines]
 - cat [files]
 - mv <src> <dst>
 - rm  [files]
 - local (to execute one of the above commands on the local FS)
 - support for piping to local commands
 - use ">" to run command output to HDFS filesystem
 - use ">l" to run command output to local filesystem

Planned future actions:
 - cp (currently can do 'cat src > dst' if desperate)

Known bugs:
 - Tab completion fails when using ".."
 - Using numLines with head breaks tab completion
 - Currently breaking on files > 100000 characters. This is a problem
   with the ring buffer used by our pipes, and is being looked into
   actively

To use HadooSh, just copy the included jar to your Hadoop cluster's
gateway, make sure you've kinit'd if necessary, and run the following:

hadoop jar HadooSh.jar HadooSh

Enjoy.


Authors: Chris Beavers and Paul Hobbs
