import boto3
from time import sleep
import subprocess
import configparser
import socket
import sys, os
import base64
import select
import paramiko
from paramiko.py3compat import u
from netaddr import IPNetwork

REGION = 'us-east-1'
KP_DIR = ''
# e.g. xxxx/dbx1000_logging
PATH = os.path.abspath(os.path.dirname(os.path.abspath(__file__)) + '/../')

import sys, time


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


# https://github.com/losDaniel/spot-connect/

def connect_to_instance(ip, keyfile, username='ec2-user', port=22, timeout=10):
    '''
    Connect to the spot instance using paramiko's SSH client 
    __________
    parameters
    - ip : string. public IP address for the instance 
    - keyfile : string. name of the private key file 
    - username : string. username used to log-in for the instance. This will usually depend on the operating system of the image used. For a list of operating systems and defaul usernames check https://alestic.com/2014/01/ec2-ssh-username/
    - port : int. the ingress port to use for the instance 
    - timeout : int. the number of seconds to wait before giving up on a connection attempt  
    '''
    
    ssh_client = paramiko.SSHClient()                                          # Instantiate the SSH Client
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy)             # Policy for automatically adding the hostname and new host key to the local `.HostKeys` object, and saving it. 
    k = paramiko.RSAKey.from_private_key_file(keyfile+'.pem')                  # Create an RSA key from the key file to avoid runtime 

    retries = 0 
    connected = False 

    sys.stdout.flush() 
    while connected==False: 
        try:
            # use the public IP address to connect to an instance over the internet, default username is ubuntu
            ssh_client.connect(ip, username=username, pkey=k, port=port, timeout=timeout)
            connected = True
            break
        except Exception as e:
            retries+=1 
            sys.stdout.write(".")
            sys.stdout.flush() 
            if retries>=5: 
                raise e  

    return ssh_client

def run_script(instance, user_name, script, cmd=False, port=22, kp_dir=None, return_output=False, timeout=0, want_exitcode=False):
    '''
    Run a script on the the given instance 
    __________
    parameters
    - instance : dict. Response dictionary from ec2 instance describe_instances method 
    - user_name : string. SSH username for accessing instance, default usernames for AWS images can be found at https://alestic.com/2014/01/ec2-ssh-username/
    - script : string. ".sh" file or linux/unix command (or other os resource) to execute on the instance command line 
    - cmd : if True, script string is treated as an individual argument 
    - port : port to use to connect to the instance 
    '''
    
    if kp_dir is None: 
        kp_dir = KP_DIR

    if cmd: 
        commands = script
    else:
        commands = open(script, 'r').read().replace('\r', '')
    print('\nExecuting script "%s"...' % str(commands))
    client = connect_to_instance(instance['PublicIpAddress'],kp_dir+'/'+instance['KeyName'],username=user_name,port=port)

    # one channel per command
    stdin, stdout, stderr = client.exec_command(commands) 
    # get the shared channel for stdout/stderr/stdin
    channel = stdout.channel

    # we do not need stdin.
    stdin.close()                 
    # indicate that we're not going to write to that channel anymore
    channel.shutdown_write()      

    # read stdout/stderr in order to prevent read block hangs
    stdout_chunks = []
    stdout_chunks.append(stdout.channel.recv(len(stdout.channel.in_buffer)).decode('utf-8'))
    print(stdout_chunks[0], end='')
    # chunked read to prevent stalls
    while not channel.closed or channel.recv_ready() or channel.recv_stderr_ready(): 
        # stop if channel was closed prematurely, and there is no data in the buffers.
        got_chunk = False
        readq, _, _ = select.select([stdout.channel], [], [], timeout)
        for c in readq:
            if c.recv_ready(): 
                stdout_chunks.append(stdout.channel.recv(len(c.in_buffer)).decode('utf-8'))
                print(stdout_chunks[-1], end='')
                got_chunk = True
            if c.recv_stderr_ready(): 
                # make sure to read stderr to prevent stall    
                stderr.channel.recv_stderr(len(c.in_stderr_buffer))  
                got_chunk = True  
        '''
        1) make sure that there are at least 2 cycles with no data in the input buffers in order to not exit too early (i.e. cat on a >200k file).
        2) if no data arrived in the last loop, check if we already received the exit code
        3) check if input buffers are empty
        4) exit the loop
        '''
        if not got_chunk \
            and stdout.channel.exit_status_ready() \
            and not stderr.channel.recv_stderr_ready() \
            and not stdout.channel.recv_ready(): 
            # indicate that we're not going to read from this channel anymore
            stdout.channel.shutdown_read()  
            # close the channel
            stdout.channel.close()
            break    # exit as remote side is finished and our bufferes are empty

    # close all the pseudofiles
    stdout.close()
    stderr.close()

    if want_exitcode:
        # exit code is always ready at this point
        return True, (''.join(stdout_chunks), stdout.channel.recv_exit_status())
    return True, ''.join(stdout_chunks)


    '''
    
    session = client.get_transport().open_session()
    session.set_combine_stderr(True)                                           # Combine the error message and output message channels

    session.exec_command(commands) #, get_pty=True)                                             # Execute a command or .sh script (unix or linux console)
    stdout = session.makefile()
    #while True:
    #    if session.exit_status_ready():
    #        if session.recv_ready():
    #            # print('done generating graph')
    #            break
    #    print(session.recv(1).decode(
    #        'utf-8'
    #    ), end='')
    #    # sleep(1)

    output = []
    for line in iter(stdout.readline, ""):
        output.append(line)
        print(line, end="")
    
    #stdout = session.makefile()                                                # Collect the output 
    
    #try:
    #    if return_output: output = ''
#
#        for line in stdout:
#            if return_output: output+=line.rstrip()+'\n'
#            else:
#                print(line.rstrip(), flush=True)                                   # Show the output 
#
#    except (KeyboardInterrupt, SystemExit):
#        print(sys.stderr, 'Ctrl-C, stopping', flush=True)                      # Keyboard interrupt 
    client.close()                                                             # Close the connection    

    if return_output: return True, ''.join(output)     
    else: return True'''

def launch_efs(system_name, region=REGION, launch_wait=3):
    '''Create or connect to an existing file system'''

    client = boto3.client('efs', region_name=region)
    
    file_systems = client.describe_file_systems(FileSystemId=system_name)['FileSystems']                    

    # If there are no file systems with the `system_name` 
    if len(file_systems)==0:                                                   

        sys.stdout.write('Creating EFS file system...')
        sys.stdout.flush()  
        
        # Create the file system 
        client.create_file_system(                                             
            FileSystemId=system_name,
            PerformanceMode='generalPurpose',
        )

        initiated=False 

        sys.stdout.write('Initializing...')
        sys.stdout.flush()  

        # Wait until the file system is detectable 
        while not initiated: 
            
            try: 
                file_system = client.describe_file_systems(FileSystemId=system_name)['FileSystems'][0]
                initiated=True
            
            except: 
                sys.stdout.write(".")
                sys.stdout.flush() 
                time.sleep(launch_wait)

        print('Detected')

    else: 
        print('...EFS file system already exists')
        file_system = file_systems[0]                                          # If the file system exists 
                
    available=False
    sys.stdout.write('Waiting for availability...')
    sys.stdout.flush() 

    while not available: 

        file_system = client.describe_file_systems(FileSystemId=system_name)['FileSystems'][0]

        if file_system['LifeCycleState']=='available':
            available=True
            print('...Available')
            
        else: 
            sys.stdout.write(".")
            sys.stdout.flush() 
            time.sleep(launch_wait)
        
    return file_system 


def retrieve_efs_mount(file_system_name, instance, new_mount=False, region='us-west-2', mount_wait=3): 
    
    # Launch or connect to an EFS 
    file_system = launch_efs(file_system_name, region=region)                  
    file_system_id = file_system['FileSystemId']
        
    # Connect and check for existing mount targets on the EFS 
    client = boto3.client('efs', region_name=region)                            
    mount_targets = client.describe_mount_targets(FileSystemId=file_system_id)['MountTargets']

    # If no mount targets are detected
    if (len(mount_targets)==0):   
        new_mount = True 

    # Setup a new mount on the file system 
    if new_mount:                                               

        sys.stdout.write('No mount target detected. Creating mount target...')
        sys.stdout.flush() 

        subnet_id = instance['SubnetId']                                       # Gather the instance subnet ID. Subnets are your personal cloud, for a full explanation see https://docs.aws.amazon.com/vpc/latest/userguide/VPC_Subnets.html
        security_group_id = instance['SecurityGroups'][0]['GroupId']           # Get the instance's security group
        
        ec2 = boto3.resource('ec2')                                            
        subnet = ec2.Subnet(subnet_id)                                         # Get the features of the subnet
        
        net = IPNetwork(subnet.cidr_block)                                     # Get the IPv4 CIDR block assigned to the subnet.
        ips = [str(x) for x in list(net[4:-1])]                                # The CIDR block is a block or range of IP addresses, we only need to assign one of these to a single mount

        ipid = 0 
        complete = False 

        while not complete: 
            try: 
                response = client.create_mount_target(                         # Create the mount target 
                    FileSystemId=file_system_id,                               # Under the file system just created 
                    SubnetId=subnet_id,                                        # Under the same subnet as the EC2 instance you've just created 
                    IpAddress=ips[ipid],                                       # Assign it the first IP Adress from the CIDR block assigned to the subnet 
                    SecurityGroups=[
                        security_group_id,                                     # Apply the security group which must have ingress rules to allow NFS client connections (enable port 2049)
                    ]
                )
                complete=True
            except Exception as e: 
                if 'IpAddressInUse' in str(e):
                    ipid+=1 
                else: 
                    raise(e) 

        initiated = False

        sys.stdout.write('Initializing...')
        sys.stdout.flush() 

        # Probe for the mount target until it is detectable 
        while not initiated: 
            try:                                                               
                mount_target = client.describe_mount_targets(MountTargetId=response['MountTargetId'])['MountTargets'][0]
                initiated = True 
            except: 
                sys.stdout.write(".")
                sys.stdout.flush() 
                time.sleep(mount_wait)

        print('Detected')

    else: 
        mount_target = mount_targets[0]
    
    instance_dns = instance['PublicDnsName']
    print('Region',region)
    print('FSID', file_system_id)
    
    filesystem_dns = file_system_id+'.efs.'+region+'.amazonaws.com'
            
    return mount_target, instance_dns, filesystem_dns

# sudo mount -t nfs -o nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2,noresvport fs-8be06f08.efs.us-east-1.amazonaws.com:/ ~/efs
def compose_mount_script(filesystem_dns):
    '''Create a script of linux commands that can be run on an instance to connect an EFS'''
    # script = 'mkdir ~/upload' + '\n'
    # script +='chmod 777 ~/upload' + '\n'
    script = ''
    script+='mkdir ~/efs &> /dev/null'+'\n'
    script+='sudo apt-get update' + '\n'
    script+='sudo apt-get install -y nfs-common' + '\n'
    script+='sudo mount -t nfs -o nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2,noresvport '+filesystem_dns+':/   ~/efs '+'\n'
    #script+='echo "#!/bin/bash" > ~/startefs.sh ' + '\n'
    #script+='echo "sudo mount -t nfs -o nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2,noresvport '+filesystem_dns+':/   ~/efs " >> ~/startefs.sh ' + '\n'
    #script+= 'chmod u+x ~/startefs.sh \n'
    #script+= 'echo "[Unit]" | sudo tee /etc/systemd/system/efs.service\n'
    #script+= 'echo "Description=Start EFS" | sudo tee -a /etc/systemd/system/efs.service\n'
    #script+= 'echo "[Service]" | sudo tee -a /etc/systemd/system/efs.service\n'
    #script+= 'echo "ExecStart=/home/ubuntu/startefs.sh" | sudo tee -a /etc/systemd/system/efs.service\n'
    #script+= 'echo "[Install]" | sudo tee -a /etc/systemd/system/efs.service\n'
    #script+= 'echo "WantedBy=multi-user.target" | sudo tee -a /etc/systemd/system/efs.service\n'
    #script+= 'sudo systemctl start efs'
    script+='cd ~/efs'+'\n'
    script+='sudo chmod go+rw .'+'\n'
    script+='mkdir ~/efs/data &> /dev/null'+'\n'
    # sudo mount -t nfs -o nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2,noresvport fs-8be06f08.efs.us-east-1.amazonaws.com:/   ~/efs 
    return script

import termios
import tty

def posix_shell(chan):
    import select

    oldtty = termios.tcgetattr(sys.stdin)
    try:
        tty.setraw(sys.stdin.fileno())
        tty.setcbreak(sys.stdin.fileno())
        chan.settimeout(0.0)

        while True:
            r, w, e = select.select([chan, sys.stdin], [], [])
            if chan in r:
                try:
                    x = u(chan.recv(1024))
                    if len(x) == 0:
                        sys.stdout.write("\r\n*** EOF\r\n")
                        break
                    sys.stdout.write(x)
                    sys.stdout.flush()
                except socket.timeout:
                    pass
            if sys.stdin in r:
                x = sys.stdin.read(1)
                if len(x) == 0:
                    break
                chan.send(x)

    finally:
        termios.tcsetattr(sys.stdin, termios.TCSADRAIN, oldtty)

interactive_shell = posix_shell

def printTotals(transferred, toBeTransferred):
    '''Print paramiko upload transfer'''
    print("Transferred: %.3f" % float(float(transferred)/float(toBeTransferred)), end="\r", flush=True)

import zipfile, os

def makeCompressed(zipname, skip_folder=['result', 'git', 'ec2', 'figs', 'tmpres']):
    path = PATH
    print('now compressing', path)
    zipf = zipfile.ZipFile(path + '/../' + zipname, 'w', zipfile.ZIP_DEFLATED)
    for root, dirs, files in os.walk(path):
        skip = False
        # print('root', root)
        for k in skip_folder:
            if k in root:
                skip = True
                break
        if not skip: # skip previous results
            for file in files:
                if '.zip' in file or '.pdf' in file or '.json' in file or '.tex' in file:
                    continue # skip result backups and figures
                print(file)
                zipf.write(os.path.join(root, file), arcname=os.path.join(root.replace(path, ''), file))


def download_from_ec2(instance, user_name, files, local_dir='.', kp_dir=None, verbose=True):
    '''
    Upload files directly to an EC2 instance. Speed depends on internet connection and not instance type. 
    __________
    parameters 
    - instance : dict. Response dictionary from ec2 instance describe_instances method 
    - user_name : string. SSH username for accessing instance, default usernames for AWS images can be found at https://alestic.com/2014/01/ec2-ssh-username/
    - files : string or list of strings. single file, list of files or directory to upload. If it is a directory end in "/" 
    - remote_dir : '.'  string.The directory on the instance where the files will be uploaded to 
    '''

    if kp_dir is None: 
        kp_dir = KP_DIR

    client = connect_to_instance(instance['PublicIpAddress'],kp_dir+'/'+instance['KeyName'], username = user_name,port=22)
    if verbose:
        print('Connected. Downloading files...')
    sftp = client.open_sftp()

    try: 
    	for f in files: 
            f = f.replace('~', '/home/%s' % user_name)
            if f[-1] == '/':
                f = f[:-1]
                # this is a directory
                print('listing', f)
                filelist = sftp.listdir(f)
                for i in filelist:
                    print('stat', i)
                    i = f + '/' + i
                    lstatout=str(sftp.lstat(i)).split()[0]
                    if 'd' not in lstatout:
                        if verbose:
                            print('Downloading %s' % str(i.split('\\')[-1]))
                        sftp.get(i, local_dir+'/'+i.split('/')[-1], callback=printTotals)
            else:
                # is a file
                if verbose:
                    print('Downloading %s' % str(f.split('/')[-1]))
                sftp.get(f, local_dir+'/'+f.split('/')[-1], callback=printTotals)

    except Exception as e:
        raise e

    if verbose:
        print('Downloaded to %s' % local_dir)
    return True 

def upload_to_ec2(instance, user_name, files, remote_dir='.', kp_dir=None, verbose=False):
    '''
    Upload files directly to an EC2 instance. Speed depends on internet connection and not instance type. 
    __________
    parameters 
    - instance : dict. Response dictionary from ec2 instance describe_instances method 
    - user_name : string. SSH username for accessing instance, default usernames for AWS images can be found at https://alestic.com/2014/01/ec2-ssh-username/
    - files : string or list of strings. single file, list of files or directory to upload. If it is a directory end in "/" 
    - remote_dir : '.'  string.The directory on the instance where the files will be uploaded to 
    '''

    if kp_dir is None: 
        kp_dir = KP_DIR

    client = connect_to_instance(instance['PublicIpAddress'],kp_dir+'/'+instance['KeyName'], username = user_name,port=22)
    if verbose:
        print('Connected. Uploading files...')
    stfp = client.open_sftp()

    try: 
    	for f in files: 
            if verbose:
                print('Uploading %s' % str(f.split('\\')[-1]))
            stfp.put(f, remote_dir+'/'+f.split('/')[-1], callback=printTotals, confirm=True)

    except Exception as e:
        raise e

    if verbose:
        print('Uploaded to %s' % remote_dir)
    return True 

def active_shell(instance, user_name='ubuntu', port=22, kp_dir=None): 
    '''
    Leave a shell active
    __________
    parameters 
    - instance : dict. Response dictionary from ec2 instance describe_instances method 
    - user_name : string. SSH username for accessing instance, default usernames for AWS images can be found at https://alestic.com/2014/01/ec2-ssh-username/
    - port : port to use to connect to the instance 
    '''    

    if kp_dir is None: 
        kp_dir = '~/.ssh'
    
    client = connect_to_instance(instance['PublicIpAddress'],kp_dir+'/'+instance['KeyName'],username=user_name,port=port)

    console = client.invoke_shell()                                            
    console.keep_this = client                                                

    session = console.get_transport().open_session()
    session.get_pty()
    session.invoke_shell()

    try:
        interactive_shell(session)

    except Exception as e: 
        print(e)
        print('Logged out of interactive session.')

    session.close() 
    return True 


def mount_efs(fs_name='fs-8be06f08', instance=None):
    try:
        mount_target, instance_dns, filesystem_dns = retrieve_efs_mount(fs_name, instance, new_mount=False, region=REGION)
    except Exception as e: 
        raise e 
        sys.exit(1) 
    print('Connecting to instance to link EFS...')
    run_script(instance, 'ubuntu', compose_mount_script(filesystem_dns), cmd=True)

def fetchScripts(fileName):
    s = open(fileName, 'r').read().split('\n')
    return s

def initialize(scriptlist, instance):
    for script in scriptlist:
        try:
            if not run_script(instance, 'ubuntu', script, cmd=True):
                break
        except Exception as e: 
            print(str(e))
            print('Script %s failed with above error' % script)

def read_user_data_from_local_config():
    user_data = config.get('EC2', 'user_data')
    if config.get('EC2', 'user_data') is None or user_data == '':
        try:
            user_data = (open(config.get('EC2', 'user_data_file'), 'r')).read()
        except:
            user_data = ''
    return user_data


def create_client():
    client = boto3.client('ec2', region_name=REGION)
    return client


def get_existing_instance_by_tag(client):
    response = client.describe_instances(
        Filters=[
            {
                'Name': 'tag:Name',
                'Values': [config.get('EC2', 'tag')],
            }
        ])

    if len(response['Reservations']) > 0:
        # print(response['Reservations'][0]['Instances'])
        # print(response['Reservations'])
        for res in response['Reservations']:
            inst = res['Instances'][0]
            if inst['State']['Code'] != 48:
                return inst
        print('Warniing: No instance is available')
        return response['Reservations'][0]['Instances'][0]
    else:
        return None


def list_all_existing_instances(client):
    response = client.describe_instances(
        # Filters=[
        #     {
        #         'Name': 'image-id',
        #         'Values': [config.get('EC2', 'ami')]
        #     }
        #]
    )
    reservations = response['Reservations']
    if len(reservations) > 0:
        r_instances = [
            inst for resv in reservations for inst in resv['Instances']]
        for inst in r_instances:
            print("Instance Id: %s | %s | %s" %
                  (inst['InstanceId'], inst['InstanceType'], inst['State']['Name']))


def get_spot_price(client):
    price_history = client.describe_spot_price_history(MaxResults=10,
                                                       InstanceTypes=[
                                                           config.get('EC2', 'type')],
                                                       ProductDescriptions=[config.get('EC2', 'product_description')])
    import pprint
    pprint.pprint(price_history)
    minPrice = 10000
    minInd = -1
    minAZ = ''
    if len(price_history['SpotPriceHistory']) == 0:
        raise 0
    for ind, priceItem in enumerate(price_history['SpotPriceHistory']):
        if float(priceItem['SpotPrice']) < minPrice:
            minPrice = float(priceItem['SpotPrice'])
            minInd = ind
            minAZ = priceItem['AvailabilityZone']
    return float(price_history['SpotPriceHistory'][minInd]['SpotPrice']), minInd, minAZ


def provision_instance(client, user_data, az):
    user_data_encode = (base64.b64encode(user_data.encode())).decode("utf-8")
    req = client.request_spot_instances(
                                        AvailabilityZoneGroup=az,
                                        InstanceCount=1,
                                        Type='one-time',
                                        InstanceInterruptionBehavior='terminate',
                                        LaunchSpecification={
                                            'SecurityGroupIds': [
                                                config.get(
                                                    'EC2', 'security_group')
                                            ],
                                            'ImageId': config.get('EC2', 'ami'),
                                            'InstanceType': config.get('EC2', 'type'),
                                            'KeyName': config.get('EC2', 'key_pair'),

                                            'UserData': user_data_encode
                                        },
                                        
                                        SpotPrice=config.get('EC2', 'max_bid')
                                        )
    print('Spot request created, status: ' +
          req['SpotInstanceRequests'][0]['State'])

    print('Waiting for spot provisioning')
    while True:
        current_req = client.describe_spot_instance_requests(
            SpotInstanceRequestIds=[req['SpotInstanceRequests'][0]['SpotInstanceRequestId']])
        if current_req['SpotInstanceRequests'][0]['State'] == 'active':
            print('Instance allocated ,Id: ',
                  current_req['SpotInstanceRequests'][0]['InstanceId'])
            instance = client.describe_instances(InstanceIds=[current_req['SpotInstanceRequests'][0]['InstanceId']])[
                'Reservations'][0]['Instances'][0]
            client.create_tags(Resources=[current_req['SpotInstanceRequests'][0]['InstanceId']],
                               Tags=[{
                                   'Key': 'Name',
                                   'Value': config.get('EC2', 'tag')
                               }]
                               )
            return instance
        print('Waiting...',
              sleep(10))



def stop_instance(client, inst):
    try:
        print('Stopping', inst['InstanceId'])
        client.stop_instances(
            InstanceIds=[inst['InstanceId']])
        print('complete (', inst['InstanceId'], ')')
        client.delete_tags(
            Resources=[
                inst['InstanceId']
            ],
            Tags=[
                {
                    'Key': 'Name',
                    'Value': config.get('EC2', 'tag')
                },
            ]
        )
    except:
        print('Failed to stop:', sys.exc_info()[0])



def destroy_instance(client, inst):
    try:
        print('Terminating', inst['InstanceId'])
        client.terminate_instances(
            InstanceIds=[inst['InstanceId']])
        print('Termination complete (', inst['InstanceId'], ')')
        client.delete_tags(
            Resources=[
                inst['InstanceId']
            ],
            Tags=[
                {
                    'Key': 'Name',
                    'Value': config.get('EC2', 'tag')
                },
            ]
        )
    except:
        print('Failed to terminate:', sys.exc_info()[0])


def wait_for_up(client, inst):
    print('Waiting for instance to come up')
    while True:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # print(inst)
        if inst['PublicIpAddress'] is None:
            inst = get_existing_instance_by_tag(client)
        try:
            if inst['PublicIpAddress'] is None:
                print('IP not assigned yet ...')
            else:
                s.connect((inst['PublicIpAddress'], 22))
                s.shutdown(2)
                print('Server is up!')
                print('Server Public IP - %s' % inst['PublicIpAddress'])

                print('ssh -i', '"' + config.get('EC2', 'key_pair') + '.pem' + '"',
                      config.get('EC2', 'username') + '@' + inst['PublicDnsName'])
                break
        except:
            print('Waiting...', sleep(10))

# def run_code(client, inst):
#     s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#     s.connect((inst['PublicIpAddress'], 22))
#     s.

def writeLabel():
    label = subprocess.check_output(["git", "describe"]).strip()
    open(PATH + '/label.txt', 'w').write(label.decode('utf-8'))


def main(action):
    client = create_client()
    if client is None:
        print('Unable to create EC2 client')
        sys.exit(0)
    inst = get_existing_instance_by_tag(client)
    user_data = read_user_data_from_local_config()

    if action == 'start':
        # print(inst)
        if inst is None or inst['State']['Name'] == 'terminated':
            spot_price, minInd, minAZ = get_spot_price(client)
            print('Spot price is ', str(spot_price))
            if spot_price > float(config.get('EC2', 'max_bid')):
                print('Spot price is too high!')
                sys.exit(0)
            else:
                print('below maximum bid, continuing')
                provision_instance(client, user_data, az=minAZ)
                inst = get_existing_instance_by_tag(client)
        wait_for_up(client, inst)
        mount_efs(config.get('EC2','fs_id'), inst)
        writeLabel()
        makeCompressed('dbx1000_logging.zip')
        upload_to_ec2(inst, 'ubuntu', ['../../dbx1000_logging.zip'], '/home/ubuntu')
        sl = fetchScripts('../scripts/prepareEC2_metal.sh')
        initialize(sl, inst)
        active_shell(inst, 'ubuntu')
    elif action == 'wait':
        wait_for_up(client, inst)
    elif action == 'copy':
        open('label.txt','wb').write(subprocess.check_output('git describe', shell=True).strip())
        branch = subprocess.check_output('git branch', shell=True).decode('utf-8')
        if not '* array-lock-free' in branch:
            print(bcolors.WARNING, 'not on array-lock-free branch', bcolors.ENDC)
            sys.exit(0)
        makeCompressed('dbx1000_logging.zip')
        upload_to_ec2(inst, 'ubuntu', ['../../dbx1000_logging.zip'], '/home/ubuntu')
        run_script(inst, 'ubuntu', 'unzip -o dbx1000_logging.zip -d dbx1000_logging', cmd=True)
    elif action == 'copyplover-result':
        open('label.txt','wb').write(subprocess.check_output('git describe', shell=True).strip())
        branch = subprocess.check_output('git branch', shell=True).decode('utf-8')
        if not '* plover' in branch:
            print(bcolors.WARNING, 'not on plover', bcolors.ENDC)
            sys.exit(0)
        makeCompressed('dbx1000_logging.zip', ['git'])
        upload_to_ec2(inst, 'ubuntu', ['../../dbx1000_logging.zip'], '/home/ubuntu')
        run_script(inst, 'ubuntu', 'unzip -o dbx1000_logging.zip -d dbx1000_logging_plover', cmd=True)    
    elif action == 'copyhdd':
        assert(config.get('EC2', 'tag')=='yu-taurus-hdd')
        open('label.txt','wb').write(subprocess.check_output('git describe', shell=True).strip())
        branch = subprocess.check_output('git branch', shell=True).decode('utf-8')
        if not '* plover' in branch:
            print(bcolors.WARNING, 'not on plover', bcolors.ENDC)
            sys.exit(0)
        makeCompressed('dbx1000_logging.zip')
        upload_to_ec2(inst, 'ubuntu', ['../../dbx1000_logging.zip'], '/home/ubuntu')
        run_script(inst, 'ubuntu', 'unzip -o dbx1000_logging.zip -d dbx1000_logging', cmd=True)
    elif action == 'copyplover':
        print(config.get('EC2', 'tag'))
        assert(config.get('EC2', 'tag')=='yu-taurus-revision')
        open('label.txt','wb').write(subprocess.check_output('git describe', shell=True).strip())
        branch = subprocess.check_output('git branch', shell=True).decode('utf-8')
        if not '* plover' in branch:
            print(bcolors.WARNING, 'not on plover', bcolors.ENDC)
            sys.exit(0)
        makeCompressed('dbx1000_logging.zip')
        upload_to_ec2(inst, 'ubuntu', ['../../dbx1000_logging.zip'], '/home/ubuntu')
        run_script(inst, 'ubuntu', 'unzip -o dbx1000_logging.zip -d dbx1000_logging_plover', cmd=True)
    elif action == 'res-zip':
        localpath = 'res/'
        if len(sys.argv) > 3:
            localpath = sys.argv[3]
        from datetime import datetime

        now = datetime.now()
        strt = now.strftime("%m-%d-%Y-%H-%M-%S")
        print('Making zip file...')
        run_script(inst, 'ubuntu', 'cd ~/efs/ && zip -r results%s.zip results' % strt,cmd=True)
        download_from_ec2(inst, 'ubuntu', ['~/efs/results%s.zip' % strt], localpath)
    elif action == 'res':
        localpath = 'res/'
        if len(sys.argv) > 3:
            localpath = sys.argv[3]
        download_from_ec2(inst, 'ubuntu', ['~/efs/results/'], localpath)
        pass
    elif action == 'fig':
        localpath = 'figs/'
        if len(sys.argv) > 3:
            localpath = sys.argv[3]
        download_from_ec2(inst, 'ubuntu', ['~/efs/figs/'], localpath)
    elif action == 'fig-zip':
        localpath = 'figs/'
        if len(sys.argv) > 3:
            localpath = sys.argv[3]
        from datetime import datetime
        now = datetime.now()
        strt = now.strftime("%m-%d-%Y-%H-%M-%S")
        print('Making zip file...')
        run_script(inst, 'ubuntu', 'cd ~/efs/ && zip -r figs%s.zip figs' % strt,cmd=True)
        download_from_ec2(inst, 'ubuntu', ['~/efs/figs%s.zip' % strt], localpath)
    elif action == 'stop' and inst is not None:
        stop_instance(client, inst)
    elif action == 'terminate' and inst is not None:
        destroy_instance(client, inst)
    elif action == 'connect-only':
        print('ssh -i', '~/.ssh/"' + config.get('EC2', 'key_pair') + '.pem' + '"',
                      config.get('EC2', 'username') + '@' + inst['PublicDnsName'])
    elif action == 'write-label':
        writeLabel()
    elif action == 'connect':
        writeLabel()
        mount_efs(config.get('EC2','fs_id'), inst)
        print('ssh -i', '~/.ssh/"' + config.get('EC2', 'key_pair') + '.pem' + '"',
                      config.get('EC2', 'username') + '@' + inst['PublicDnsName'])
    elif action == 'list':
        print('EC2 Instnaces:')
        list_all_existing_instances(client)
    elif action == 'postprocess':
        figDir = 'figs'
        if len(sys.argv) > 3:
            figDir = sys.argv[3]
        import json
        ##### check YCSB LV overhead
        def checkLVOverheadPercentage(workload):
            lvOverheadData = json.load(open('%s/Ln16-%s-Lr0-Number-of-Worker-Threads-vs-time-locktable-get-per-thd.json' % (figDir, workload), 'r'))
            perThdRunningTime = json.load(open('%s/Ln16-%s-Lr0-Number-of-Worker-Threads-vs-run-time-per-thd.json' % (figDir, workload)))
            lvOverhead = (lvOverheadData['Taurus Command 2PL'][1][-1] - lvOverheadData['No Logging Data 2PL'][1][-1])
            return lvOverhead / perThdRunningTime['Taurus Command 2PL'][1][-1]
        print(bcolors.OKGREEN, "Checking LV Overhead Percentage based on No Logging...", bcolors.ENDC)
        print('YCSB', checkLVOverheadPercentage('YCSB'))
        print('Tm0-TPCC', checkLVOverheadPercentage('Tm0-TPCC'))
        print('Tm1-TPCC', checkLVOverheadPercentage('Tm1-TPCC'))
        def checkLVOverheadPercentageWithSerial(workload):
            lvOverheadData = json.load(open('%s/Ln16-%s-Lr0-Number-of-Worker-Threads-vs-Throughput.json' % (figDir, workload),'r'))
            return (lvOverheadData['Serial Command 2PL'][1][-1] - lvOverheadData['Taurus Command 2PL'][1][-1]) / lvOverheadData['Serial Command 2PL'][1][-1]
        print(bcolors.OKGREEN, "Checking LV Overhead Percentage based on Serial...", bcolors.ENDC)
        print('Tm0-TPCC', checkLVOverheadPercentageWithSerial('Tm0-TPCC'))

    else:
        print('No action taken')


if __name__ == "__main__":

    action = 'stop' if len(sys.argv) == 1 else sys.argv[1]
    config_file = 'config.ini'
    if len(sys.argv) == 3:
        config_file = sys.argv[2]

    config = configparser.ConfigParser()
    config.read(config_file)
    main(action)
    