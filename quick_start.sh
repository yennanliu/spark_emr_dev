quick_start() {

    echo "### HELP SCRIPT RUN REPO COMMANDS ### "
    echo ""

    echo "install_sbt_at_emr : " "bash quick_start.sh  install_sbt_at_emr"
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
    if [ -n `which sbt` ]; then 
            echo "sbt alrady installed"
            which sbt
        else
        cd ~
        wget https://github.com/sbt/sbt/releases/download/v0.13.15/sbt-0.13.15.tgz
        tar xf sbt-0.13.15.tgz
        sudo mv sbt /opt
        export PATH=$PATH:/opt/sbt/bin
        echo "run sbt test..."
        sbt test
    fi
    set -e
}

install_java(){
  set +e
  if [ -n `which java` ]; then 
  echo 'java install OK'
  which java
  java -version  
  else 
  echo 'No java installed, please install it for running spark'
  echo 'install java 8 OpenJDK via apt...'
  apt install openjdk-8-jdk
  fi 
  set -e 

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
