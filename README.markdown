# JEX

Backend service that accepts JSON and submits DAGs to Condor with condor_submit_dag.

# Configuration

The JEX is intended to be run as a user that can submit jobs to a Condor cluster. The condor_submit_dag executable needs to be on the PATH for the user that JEX runs as.
