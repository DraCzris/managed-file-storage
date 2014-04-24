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
    );


    public function loadConfiguration()
    {
        $config = $this->getConfig($this->defaults);

        $builder = $this->getContainerBuilder();

        $builder->agetDefinition('cacheStorage') // no namespace for back compatibility
            ->setClass(
                'Draczris\ManagedFileStorage',
                array($container->expand('%tempDir%/cache', '@cacheJournal', $config['ownership']))
                );
    }

}


class InvalidConfigException extends Nette\InvalidStateException {

}
