docker kill $(docker container ls -q --filter name="tp1-grouper*" --filter name="tp1-joiner*" --filter name="tp1-processor_media_general*" --filter name="tp1-processor_dos_mas_rapidos*")
