<?php

/*
 * This file is part of the sfPropelMigrationsLightPlugin package.
 * (c) 2006-2008 Martin Kreidenweis <sf@kreidenweis.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

/**
 * Manage all calls to the sfMigration class instances.
 *
 * @package    symfony
 * @subpackage plugin
 * @author     Martin Kreidenweis <sf@kreidenweis.com>
 * @version    SVN: $Id: sfMigrator.class.php 26873 2010-01-19 12:09:31Z Stefan.Koopmanschap $
 */
class sfMigrator
{
  /**
   * Migration filenames.
   *
   * @var array $migrations
   */
  protected $migrations = array();

  /**
   * Perform an update on the database.
   *
   * @param   string $sql
   *
   * @return  integer
   */
  static public function executeUpdate($sql)
  {
    $con = Propel::getConnection();

    return $con instanceof PropelPDO ? $con->exec($sql) : $con->executeUpdate($sql);
  }

  /**
   * Perform a query on the database.
   *
   * @param   string $sql
   * @param   string $fetchmode
   *
   * @return  mixed
   */
  static public function executeQuery($sql, $fetchmode = null)
  {
    $con = Propel::getConnection();

    if ($con instanceof PropelPDO)
    {
      $stmt = $con->prepare($sql);
      $stmt->execute();

      return $stmt;
    }
    else
    {
      return $con->executeQuery($sql, $fetchmode);
    }
  }

  /**
   * Constructor.
   */
  public function __construct()
  {
    $this->loadMigrations();
  }

  /**
   * Execute migrations.
   *
   * @param   integer $destVersion  Version number to migrate to, defaults to
   *                                the max existing
   *
   * @return  integer Number of executed migrations
   */
  public function migrate($destVersion = null)
  {
    $maxVersion = $this->getMaxVersion();
    if ($destVersion === null)
    {
      $destVersion = $maxVersion;
    }
    else
    {
      $destVersion = $destVersion;

      if (($destVersion > $maxVersion) || ($destVersion < 0))
      {
        throw new sfException(sprintf('Migration %d does not exist.', $destVersion));
      }
    }

    $sourceVersion = $this->getCurrentVersion();
    
    // do appropriate stuff according to migration direction
    if (strnatcmp($destVersion, $sourceVersion) == -1)
    {
      $res = $this->migrateDown($sourceVersion, $destVersion);
    }
    else
    {
      $res = $this->migrateUp($sourceVersion, $destVersion);
    }

    return $res;
  }
  
  public function rollback($steps=1)
  {
    $current_version = $this->getCurrentVersion();
    $result = $this->executeQuery("SELECT version FROM schema_migration WHERE version != '$current_version' ORDER BY lpad(version, 14, '0') desc limit 1");
    if($result instanceof PDOStatement)
    {
      $prev_version = $result->fetchColumn(0);
    }
    else
    {
      if($result->next())
      {
        $prev_version = $result->getString('version');
      }
      else
      {
        throw new sfDatabaseException('Unable to retrieve current schema version.');
      }
    }
    
    return $this->migrate($prev_version);
  }

  /**
   * Generate a new migration stub
   *
   * @param   string $name Name of the new migration
   *
   * @return  string Filename of the new migration file
   */
  public function generateMigration($name)
  {
    // calculate version number for new migration
    $maxVersion = $this->getMaxVersion();
    $newVersion = date("YmdHis");

    // sanitize name
    $name = preg_replace('/[^a-zA-Z0-9]/', '_', $name);

    $upLogic = '';
    $downLogic = '';

    if($maxVersion == 0)
    {
      $this->generateFirstMigrationLogic($name, $newVersion, $upLogic, $downLogic);
    }

    $newClass = <<<EOF
<?php

/**
 * Migrations between versions $maxVersion and $newVersion.
 */
class Migration$newVersion extends sfMigration
{
  /**
   * Migrate up to version $newVersion.
   */
  public function up()
  {
    $upLogic
  }

  /**
   * Migrate down to version $maxVersion.
   */
  public function down()
  {
    $downLogic
  }
}

EOF;

    // write new migration stub
    $newFileName = $this->getMigrationsDir().DIRECTORY_SEPARATOR.$newVersion.'_'.$name.'.php';
    file_put_contents($newFileName, $newClass);

    return $newFileName;
  }

  /**
   * Get the list of migration filenames.
   *
   * @return array
   */
  public function getMigrations()
  {
    return $this->migrations;
  }

  /**
   * @return integer The lowest migration that exists
   */
  public function getMinVersion()
  {
    return $this->migrations ? $this->getMigrationNumberFromFile($this->migrations[0]) : 0;
  }

  /**
   * @return integer The highest existing migration that exists
   */
  public function getMaxVersion()
  {
    $count = count($this->migrations);
    $versions = array_keys($this->migrations);

    return $count ? $this->getMigrationNumberFromFile($versions[$count - 1]) : 0;
  }

  /**
   * Get the current schema version from the database.
   *
   * If no schema version is currently stored in the database, one is created.
   *
   * @return integer
   */
  public function getCurrentVersion()
  {
    try
    {
      $result = $this->executeQuery("SELECT max(lpad(version, 14, '0')) as version FROM schema_migration");
      if($result instanceof PDOStatement)
      {
        $currentVersion = $result->fetchColumn(0);
      }
      else
      {
        if($result->next())
        {
          $currentVersion = $result->getString('version');
        }
        else
        {
          throw new sfDatabaseException('Unable to retrieve current schema version.');
        }
      }
    }
    catch (Exception $e)
    {
      // assume no schema_info table exists yet so we create it
      $this->executeUpdate('CREATE TABLE `schema_migration` (
        `version` varchar(255) NOT NULL,
        UNIQUE KEY `unique_schema_migrations` (`version`)
      )');

      // attempt to migrate the schema_version table
      $currentVersion = $this->migrateSchemaVersion();
    }

    return $this->cleanVersion($currentVersion);
  }
  
  public function migrateSchemaVersion()
  {
    try {
      $result = $this->executeQuery("SELECT version FROM schema_info");
      if ($result instanceof PDOStatement)
      {
        $currentVersion = $result->fetchColumn(0);
      }
      else
      {
        if($result->next())
        {
          $currentVersion = $result->getInt('version');
        }
        else
        {
          throw new sfDatabaseException('Unable to retrieve current schema version.');
        }
      }
      
      for($i = 0; $i <= $currentVersion; $i++)
        $this->executeUpdate("INSERT INTO schema_migration(version) VALUES ($i)");
        
      return $currentVersion;
    } catch(Exception $e) {
      // ignore it; assume that the old table doesn't exist
    }
    
    return 0;
  }

  /**
   * Get the number encoded in the given migration file name.
   *
   * @param   string $file The filename to look at
   *
   * @return  integer
   */
  public function getMigrationNumberFromFile($file)
  {
    $number = current(explode("_", basename($file), 2));

    if (!ctype_digit($number))
    {
      throw new sfParseException('Migration filename could not be parsed.');
    }

    return $number;
  }

  /**
   * Get the directory where migration classes are saved.
   *
   * @return  string
   */
  public function getMigrationsDir()
  {
    return sfConfig::get('sf_data_dir').DIRECTORY_SEPARATOR.'migrations';
  }

  /**
   * Get the directory where migration fixtures are saved.
   *
   * @return  string
   */
  public function getMigrationsFixturesDir()
  {
    return $this->getMigrationsDir().DIRECTORY_SEPARATOR.'fixtures';
  }

  /**
   * Write the given version as current version to the database.
   *
   * @param integer $version New current version
   */
  protected function recordMigration($version)
  {
    $version = $this->cleanVersion($version);
    $this->executeUpdate("INSERT INTO schema_migration(version) VALUES ($version)");
    
    pake_echo_action('migrations', "migrated version $version");
  }
  
  protected function unrecordMigration($version)
  {
    $version = $this->cleanVersion($version);
    $this->executeUpdate("DELETE FROM schema_migration WHERE version='$version'");
    
    pake_echo_action('migrations', "rollback version $version");
  }
  

  /**
   * Migrate down, from version $from to version $to.
   *
   * @param   integer $from
   * @param   integer $to
   *
   * @return  integer Number of executed migrations
   */
  protected function migrateDown($from, $to)
  {
    $counter = 0;
    
    // look for any unapplied migrations with versions between the from:to range.
    foreach($this->migrations as $version => $migration)
    {
      if(strnatcmp($this->cleanVersion($version), $to) > 0)
      {
        $counter += $this->doMigrateDown($version);
      }
    }

    return $counter;
  }
  
  public function cleanVersion($version)
  {
    return preg_replace("/^0*/", "", $version);
  }
  
  public function doMigrateDown($version)
  {
    $con = Propel::getConnection();
    
    if(!$this->isMigrationApplied($version))
      return 0;
      
    try
    {
      $con instanceof PropelPDO ? $con->beginTransaction() : $con->begin();

      $migration = $this->getMigrationObject($version);
      $migration->down();

      $this->unrecordMigration($version);

      $con->commit();
    }
    catch (Exception $e)
    {
      $con->rollback();
      throw $e;
    }
    
    return 1;
  }

  /**
   * Migrate up, from version $from to version $to.
   *
   * @param   integer $from
   * @param   integer $to
   * @return  integer Number of executed migrations
   */
  protected function migrateUp($from, $to)
  {
    $counter = 0;

    // look for any unapplied migrations with versions between the from:to range.
    foreach($this->migrations as $version => $migration)
    {
      if(strnatcmp($version, $to) <= 0)
      {
        $counter += $this->doMigrateUp($version);
      }
    }

    return $counter;
  }
  
  public function doMigrateUp($version)
  {
    if($this->isMigrationApplied($version))
      return 0;
      
    $con = Propel::getConnection();
    
    try
    {
      $con instanceof PropelPDO ? $con->beginTransaction() : $con->begin();

      $migration = $this->getMigrationObject($version);
      $migration->up();

      $this->recordMigration($version);

      $con->commit();
    }
    catch (Exception $e)
    {
      $con->rollback();
      throw $e;
    }
    
    return 1;
  }

  /**
   * Get the migration object for the given version.
   *
   * @param   integer $version
   *
   * @return  sfMigration
   */
  protected function getMigrationObject($version)
  {
    $file = $this->getMigrationFileName($version);

    // load the migration class
    require_once $file;
    $migrationClass = 'Migration'.$this->getMigrationNumberFromFile($file);

    return new $migrationClass($this, $version);
  }

  /**
   * Version to filename.
   *
   * @param   integer $version
   *
   * @return  string Filename
   */
  protected function getMigrationFileName($version)
  {
    return $this->migrations[$version];
  }

  /**
   * Load all migration file names.
   */
  protected function loadMigrations()
  {
    $migrations = sfFinder::type('file')->name('/^\d{3}.*\.php$/')->maxdepth(0)->in($this->getMigrationsDir());
    sort($migrations);
    foreach($migrations as $migration)
    {
      $this->migrations[current(explode('_', basename($migration), 2))] = $migration;
    }
    
    // grab 
  }
  
  public function isMigrationApplied($version)
  {
    $version = $this->cleanVersion($version);
    $result = $this->executeQuery("SELECT count(*) as c FROM schema_migration WHERE version='$version'");
    if($result instanceof PDOStatement)
    {
      $count = $result->fetchColumn(0);
    }
    else
    {
      if($result->next())
      {
        $count = $result->getString('c');
      }
      else
      {
        throw new sfDatabaseException('Unable to retrieve version info.');
      }
    }
    
    return $count > 0;
  }

  /**
   * Auto generate logic for the first migration.
   *
   * @param   string $name
   * @param   string $newVersion
   * @param   string $upLogic
   * @param   string $downLogic
   */
  protected function generateFirstMigrationLogic($name, $newVersion, &$upLogic, &$downLogic)
  {
    $sqlFiles = sfFinder::type('file')->name('*.sql')->in(sfConfig::get('sf_root_dir').'/data/sql');
    if ($sqlFiles)
    {
      // use propel sql files for the up logic
      $sql = '';
      foreach ($sqlFiles as $sqlFile)
      {
        $sql .= file_get_contents($sqlFile);
      }
      file_put_contents($this->getMigrationsDir().DIRECTORY_SEPARATOR.$newVersion.'_'.$name.'.sql', $sql);
      $upLogic .= sprintf('$this->loadSql(dirname(__FILE__).\'/%s_%s.sql\');', $newVersion, $name);

      // drop tables for down logic
      $downLines = array();

      // disable mysql foreign key checks
      if (false !== $fkChecks = strpos($sql, 'FOREIGN_KEY_CHECKS'))
      {
        $downLines[] = '$this->executeSQL(\'SET FOREIGN_KEY_CHECKS=0\');';
        $downLines[] = '';
      }

      preg_match_all('/DROP TABLE IF EXISTS `(\w+)`;/', $sql, $matches);
      foreach ($matches[1] as $match)
      {
        $downLines[] = sprintf('$this->executeSQL(\'DROP TABLE %s\');', $match);
      }

      // enable mysql foreign key checks
      if (false !== $fkChecks)
      {
        $downLines[] = '';
        $downLines[] = '$this->executeSQL(\'SET FOREIGN_KEY_CHECKS=1\');';
      }

      $downLogic .= join("\n    ", $downLines);
    }
  }
}
