<?php

namespace App\Console\Commands;

use League\Csv\Reader;
use League\Csv\Statement;
use League\Csv\Writer;

use Illuminate\Console\Command;
use Illuminate\Foundation\Bus\DispatchesJobs;
use Illuminate\Support\Facades\Storage;
use Validator;

use App\Leads;

class LeadsParser extends Command
{
    use DispatchesJobs;

    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'parser:leads_parse';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Algorithm for parsing leads from csv tables with further processing and synchronization with the database';

    private $rules = [];
    private $maps = [];
    
    /**
     * Create a new command instance.
     *
     * @return void
     */
    public function __construct()
    {
        parent::__construct();
        
        $this->maps = [
            'identifier' => 'id',
            'name' => 'name',
            'last name' => 'lastname',
            'card' => 'card',
            'email' => 'email',
        ];
        $this->rules = [
            'id' => 'required|integer',
            'name' => 'required|string|min:2|max:50',
            'lastname' => 'required|string|min:2|max:50',
            'card' => 'required|integer',
            'email' => 'required|email|max:80',
        ];
    }

    /**
     * Execute the console command.
     *
     * @return mixed
     */
    public function handle()
    {
        $status = 'failed';
        $resultFinal = [
            'new' => 0,
            'deleted' => 0,
            'restored' => 0,
            'updated' => 0,
            'rejected' => 0,
        ];
        $logsLines = [
            'rejected_lines' => [],
            'new_lines' => [],
            'updated_lines' => [],
            'deleted_lines' => [],
            'restored_lines' => [],
        ];
        
        $allFiles = Storage::files('leads');
        if (!empty($allFiles)) {
            $matchingFiles = preg_grep('#^leads/(.*?.csv)#', $allFiles);
            if (!empty($matchingFiles[0])) {
                $file = Storage::path($matchingFiles[0]);
                
                $csv = Reader::createFromPath($file, 'r');
                $csv->setDelimiter(',');
                $csv->setHeaderOffset(0);
                $csv->skipEmptyRecords();
                
                // Remove whitespace and strig to lower cases
                $headers = array_map('trim', $csv->getHeader());
                $headers = array_map('strtolower', $headers);
                
                $records = collect($csv->getRecords($headers));
                
                // Rename keys
                $records->transform(function ($value, $key) {
                    $return = [];
                    foreach ($value as $name => $data) {
                        $newName = $this->maps[$name];
                        $return[$newName] = $data;
                    }
                    return $return;
                });
                
                // In case there is a duplicate, we reject both.
                $dublicates = [];
                $records->each(function($value, $key) use ($records, $resultFinal, &$dublicates) {
                    $records->each(function($valueFilter, $keyFilter) use ($value, $key, $resultFinal, &$dublicates) {
                        if ($key == $keyFilter) {
                            return true;
                        }
                        if ($valueFilter['id'] == $value['id'] || $valueFilter['card'] == $value['card']) {
                            $dublicates[] = $key;
                            $dublicates[] = $keyFilter;
                            return false;
                        }
                        return true;
                    });
                });
                
                foreach (array_unique($dublicates) as $value) {
                    $records->pull($value);
                    $resultFinal['rejected']++;
                }
                
                // In case some row fails in validation - reject only this row
                $records = $records->filter(function($value, $key) use (&$resultFinal, &$logsLines) {
                    $validator = Validator::make($value, $this->rules);
                    if ($validator->fails()) {
                        $error = '';
                        foreach ($validator->messages()->getMessages() as $key => $messages) {
                            $error .=  'Validation field [' . $key . '] ';
                            foreach ($messages as $message) {
                                $error .=  'Error: ' . $message . ' ';
                            }
                            $error .= PHP_EOL;
                        }
                        $logsLines['rejected_lines'][] = array_merge($value, ['error' => $error]);
                        $resultFinal['rejected']++;
                        
                        return false;
                    }
                    return true;
                });
                
                
                $leads = Leads::withTrashed()->get();
                $records->each(function($value, $key) use ($leads, &$resultFinal, &$logsLines) {
                    $lead = $leads->where('id', $value['id']);
                    $card = $leads->where('card', $value['card']);
                    
                    if (!$lead->isEmpty()) {
                        $leadArray = $lead->first()->toArray();
                        
                        // TODO A moment not described in the task, if in the database one of the 
                        // records contains a unique id, and the other has a unique card
                        if ($leadArray['card'] == $value['card'] || $card->isEmpty()) {
                            $preSaveData = collect($value)->except(['id'])->all();
                            $preSaveDataWithId = collect($value)->all();
                            
                            $saveData = [];
                            foreach ($preSaveData as $saveKey => $saveValue) {
                                if ($saveValue != $leadArray[$saveKey]) {
                                    $saveData[$saveKey] = $saveValue;
                                }
                            }
                            
                            $oldFields = [];
                            foreach($leadArray as $nameField => $valueField) {
                                
                                if (in_array($nameField, array_merge(array_keys($saveData)))) {
                                    $oldFields[$nameField . '-old'] = $valueField;
                                }
                            }
                            
                            $dbLead = Leads::withTrashed()->find($value['id']);
                            if ($dbLead->trashed()) {
                                
                                // Restore item
                                $dbLead->restore();
                                
                                $logsLines['restored_lines'][] = array_merge(['id' => $preSaveDataWithId['id']], $saveData, $oldFields);
                                $resultFinal['restored']++;
                            } else {
                                
                                if (!empty($saveData)) {
                                    $logsLines['updated_lines'][] = array_merge(['id' => $preSaveDataWithId['id']], $saveData, $oldFields);
                                    $resultFinal['updated']++;
                                }
                            }
                            
                            // Update item
                            if (!empty($saveData)) {
                                $dbLead = Leads::where('id', $value['id'])->update($saveData);
                            }
                        } else {
                            $resultFinal['rejected']++;
                        }
                    } else {
                        
                        // Insert item
                        Leads::insert($value);
                        
                        $logsLines['new_lines'][] = $value;
                        $resultFinal['new']++;
                    }
                });
                
                // Remove items in database
                $leads->each(function($value, $key) use ($records, &$resultFinal, &$logsLines) {
                    $record = $records->where('id', $value['id']);
                    if ($record->isEmpty()) {
                        $dbLead = Leads::find($value['id']);
                        if (!empty($dbLead)) {
                            $dbLead->delete();
                            
                            $logsLines['deleted_lines'][] = $value;
                            $resultFinal['deleted']++;
                        } else {
                            //$resultFinal['updated']++;
                        }
                    }
                });
                
                $status = 'passed';
            }
        }
        
        echo '-------------' . PHP_EOL;
        echo 'status: ' . $status . PHP_EOL;
        foreach ($resultFinal as $key => $value) {
            echo $key . ': ' . $value . ' ';
        }
        echo PHP_EOL . '-------------' . PHP_EOL;
        
        foreach ($logsLines as $key => $value) {
            if (!empty($value)) {
                $folder = Storage::path('leads/logs');
                $writer = Writer::createFromPath($folder . '/' . $key . '-' . date("Ymd-His") . '.csv', 'w+');
                $writer->insertOne(array_keys($value[0]));
                $writer->insertAll($value);
            }
        }
    }
}
