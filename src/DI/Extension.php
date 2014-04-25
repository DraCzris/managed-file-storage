<?php

namespace ManagedFileStorage\DI;

use Nette;


if (!class_exists('Nette\DI\CompilerExtension')) {
    class_alias('Nette\Config\CompilerExtension', 'Nette\DI\CompilerExtension');
}


class Extension extends Nette\DI\CompilerExtension
{
    /** @var array */
    private $defaults = array(
        'ownership' => 'www-data:www-data',
        'useSudo' => false
    );


    public function loadConfiguration()
    {
        $config = $this->getConfig($this->defaults);

        $builder = $this->getContainerBuilder();

        $builder->addDefinition('filePermissionsManager')
            ->setClass(
                'ManagedFileStorage\FilePermissionsManager',
                array($config['ownership'], $config['useSudo'])
            );

        $builder->addDefinition('managedFileJournal')
            ->setClass(
                'ManagedFileStorage\ManagedFileJournal',
                array($builder->expand('%tempDir%'), "@filePermissionsManager")
            )->setAutowired(FALSE);

        $builder->addDefinition('managedFileStorage')
            ->setClass(
                'ManagedFileStorage\ManagedFileStorage',
                array($builder->expand('%tempDir%/cache'), "@filePermissionsManager", "@managedFileJournal")
            );
    }
}


class InvalidConfigException extends Nette\InvalidStateException {

}
