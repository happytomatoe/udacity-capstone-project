# Create redshift cluster
1) Go to parent folder
2) Run jupyter notebook to open notebook in this folder(make run from parent folder)
3) Copy/mv dwh-template.cfg to dwg.cfg
4) Set the variables in the configuration file 
5) Go to the notebook and create the cluster 

# Note
If you launch redshift for the first time add inbound rule to allow traffic to redshift :
AWS Console>Redshift cluster page> Click 'properties' tab > Click on VCP security group >
Edit inbound rules and add the rule

In the 'type' field select redshift 