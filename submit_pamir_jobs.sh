
for YEAR_START in $(seq 1960 8 2024); do

    YEAR_END=$(($YEAR_START+7))
    export YEAR_START
    export YEAR_END
    export DOCKER_TAG=$YEAR_START-$YEAR_END
    
    sed -i.bak -e "s/\(start_year: \)[0-9]\{4\}/\1$YEAR_START/" -e "s/\(end_year: \)[0-9]\{4\}/\1$YEAR_END/" ./requests/cryogrid_pamir_region.yml
    
    make build DOCKER_BUILD_FLAGS='--no-cache'
    make push
    make runai-submit
done
