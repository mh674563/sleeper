/*
 * Copyright 2022 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package sleeper.environment.cdk.buildec2;

import sleeper.environment.cdk.config.AppContext;
import sleeper.environment.cdk.util.MyIpUtil;
import software.amazon.awscdk.CfnOutput;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;
import software.amazon.awscdk.services.ec2.*;
import software.constructs.Construct;

import java.security.KeyPair;
import java.util.Collections;
import java.util.UUID;

import static sleeper.environment.cdk.config.AppParameters.VPC;

public class BuildEC2Stack extends Stack {

    private final IVpc vpc;

    public BuildEC2Stack(Construct scope, StackProps props, IVpc inheritVpc) {
        super(scope, props.getStackName(), props);
        AppContext context = AppContext.of(this);
        BuildEC2Parameters params = BuildEC2Parameters.from(context);
        vpc = context.getOrDefault(VPC, this, inheritVpc);

        CfnKeyPair key = createSshKeyPair();
        SecurityGroup allowSsh = createAllowSshSecurityGroup();

        Instance instance = Instance.Builder.create(this, "EC2")
                .vpc(vpc)
                .securityGroup(allowSsh)
                .machineImage(MachineImage.lookup(LookupMachineImageProps.builder()
                        .name("ubuntu/images/hvm-ssd/ubuntu-focal-20.04-amd64-server-*")
                        .owners(Collections.singletonList("099720109477"))
                        .build()))
                .instanceType(InstanceType.of(InstanceClass.T3, InstanceSize.LARGE))
                .vpcSubnets(SubnetSelection.builder().subnetType(SubnetType.PUBLIC).build())
                .userData(UserData.custom(LoadUserDataUtil.userData(params)))
                .userDataCausesReplacement(true)
                .keyName(key.getKeyName())
                .blockDevices(Collections.singletonList(BlockDevice.builder()
                        .deviceName("/dev/sda1")
                        .volume(BlockDeviceVolume.ebs(200,
                                EbsDeviceOptions.builder().volumeType(EbsDeviceVolumeType.GP3).build()))
                        .build()))
                .build();
        instance.getInstance().addDependsOn(key);

        CfnOutput.Builder.create(this, "ConnectCommand")
                .value("ssh -i BuildEC2.pem ubuntu@" + instance.getInstancePublicIp())
                .description("Command to connect to EC2")
                .build();
    }

    private CfnKeyPair createSshKeyPair() {
        // Create a new SSH key every time the CDK is run.
        // A UUID is appended to the key name to ensure that the key is deleted and recreated every time the stack is
        // deployed. This is because AWS does not permit updating key pairs in-place, and the library we're using does
        // not handle that.
        KeyPair keyPair = KeyPairUtil.generate();
        KeyPairUtil.writePrivateToFile(keyPair, getStackName() + ".pem");
        return CfnKeyPair.Builder.create(this, "KeyPair")
                .keyName(getStackName() + "-" + UUID.randomUUID())
                .publicKeyMaterial(KeyPairUtil.publicBase64(keyPair))
                .build();
    }

    private SecurityGroup createAllowSshSecurityGroup() {
        SecurityGroup allowSsh = SecurityGroup.Builder.create(this, "AllowSsh")
                .vpc(vpc)
                .description("Allow SSH inbound traffic")
                .allowAllOutbound(true)
                .build();
        allowSsh.addIngressRule(Peer.ipv4(MyIpUtil.findMyIp() + "/32"), Port.tcp(22));
        return allowSsh;
    }

}
