quick_start() {

    echo "### HELP SCRIPT RUN REPO COMMANDS ### "
    echo ""

    echo "install_sbt_at_emr : " "bash quick_start.sh  install_sbt_at_emr"
    echo "install_git_at_emr : " "bash quick_start.sh  install_git_at_emr"
    echo "clone_repo         : " "bash quick_start.sh  clone_repo"
    echo "run_sbt_test       : " "bash quick_start.sh  run_sbt_test"
    echo "run_sbt_compile    : " "bash quick_start.sh  run_sbt_compile"
    echo "run_sbt_run        : " "bash quick_start.sh  run_sbt_run"
    echo "run_sbt_test       : " "bash quick_start.sh  run_sbt_test"
    echo "run_sbt_assembly   : " "bash quick_start.sh  run_sbt_assembly"

    echo ""
    echo "### HELP SCRIPT RUN REPO COMMANDS ### "
}

install_sbt_at_emr() {

    set +e
    cd ~ 
    export PATH=$PATH:/opt/sbt/bin
    cd ~
    wget https://github.com/sbt/sbt/releases/download/v0.13.15/sbt-0.13.15.tgz
    tar xf sbt-0.13.15.tgz
    sudo mv sbt /opt
    export PATH=$PATH:/opt/sbt/bin
    echo "run sbt test..."
    sbt test
    set -e
    # if [ -n `which sbt` ]; then 
    #         echo "sbt alrady installed"
    #         which sbt
    #     else
    #     cd ~
    #     wget https://github.com/sbt/sbt/releases/download/v0.13.15/sbt-0.13.15.tgz
    #     tar xf sbt-0.13.15.tgz
    #     sudo mv sbt /opt
    #     export PATH=$PATH:/opt/sbt/bin
    #     echo "run sbt test..."
    #     sbt test
    # fi
}

install_git_at_emr(){

    set +e
    sudo yum install -y \
      git \
      autoconf-2.69-11.9.amzn1.noarch \
      automake-1.13.4-3.15.amzn1.noarch \
      protobuf-compiler \
      protobuf \
      protobuf-devel
    set -e

}

clone_repo(){

    cd ~ 
    git clone https://github.com/yennanliu/spark_emr_dev.git
    cd spark_emr_dev
    pwd
    ls
}

run_sbt_test() {

    sbt test
}

run_sbt_compile() {

    sbt clean compile
}

run_sbt_run() {

    sbt run 
}

run_sbt_assembly() {

sbt clean compile && sbt assembly 

}

# run specific bash function via CLI
#https://superuser.com/questions/106272/how-to-call-bash-functions
quick_start
$1 $2 $3 $4 $5 
