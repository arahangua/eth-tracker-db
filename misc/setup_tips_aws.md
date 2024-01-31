## notes for setting up docker DBs using amazon linux

1. install git 

```
sudo dnf update
sudo dnf install git -y
```

2. setup ssh connection (git)
```
# keygen
ssh-keygen -t ed25519 -C "your_email@example.com"

# eval agent
eval "$(ssh-agent -s)"

# add private key to the agent
ssh-add ~/.ssh/id_ed25519

# get pubkey
cat ~/.ssh/id_ed25519.pub
```

3. install docker
```
sudo yum update -y
sudo yum install docker -y

#start docker
sudo systemctl start docker

#check installation
sudo docker run hello-world
docker --version

#enable docker (bootup setting)
sudo systemctl enable docker

# add user to the docker group
sudo usermod -a -G docker $(whoami)
newgrp docker
```

4. setting mount folder permission for neo4j

```
sudo chown -R $USER:$GROUP <mount folder>
chmod -R 755 <mount folder>
```