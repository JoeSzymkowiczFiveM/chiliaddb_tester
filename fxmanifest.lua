fx_version 'cerulean'
use_experimental_fxv2_oal 'yes'
game 'gta5'

name 'chiliaddb_tester'
author 'JoeSzymkowiczFivem'
license 'CC0 1.0 Universal (CC0 1.0)'
description 'Unit testing resource for ChiliadDB'

dependencies {
    'chiliaddb',
    'ox_lib'
}

server_scripts {
    '@chiliaddb/init.lua',
    '@ox_lib/init.lua',
    'main.lua'
}
