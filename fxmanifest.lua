fx_version   'cerulean'
use_experimental_fxv2_oal 'yes'
lua54        'yes'
game         'gta5'

name 'chiliaddb_tester'
author 'JoeSzymkowiczFivem'
license 'CC0 1.0 Universal (CC0 1.0)'
description 'Unit testing resource for ChiliadDB'

dependency 'chiliaddb'

server_scripts {
    '@chiliaddb/init.lua',
	'main.lua'
}