docker kill $(docker container ls -q --filter name="tp1-filter_dos_mas_rapidos*" --filter name="tp1-processor_dos_mas_rapidos*" --filter name="tp1-tagger_dos_mas_rapidos*")
