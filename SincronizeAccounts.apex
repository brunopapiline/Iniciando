public with sharing class SincronizeAccounts {

    @future(callout=true)
    public static void runSync() {
        // Limite de registros do Custom Setting
        Integer recordLimit = getRecordLimitFromCustomSetting();
        Integer batchSize = 200;

        if (recordLimit == null) {
            logMissingCustomSetting();
            return; // Interrompe o processo se o Custom Setting estiver vazio
        }

        // Verifica se o limite já foi atingido
        Integer totalAccounts = [SELECT COUNT() FROM Account];
        if (totalAccounts >= recordLimit) {
            logIntegrationEvent(
                'ImportAccount',
                'Processo interrompido. Limite de registros da organização já foi atingido: ' + recordLimit,
                null
            );
            return; // Interrompe o job
        }

        enqueueSyncJob(1, batchSize, recordLimit, totalAccounts);
    }

    private static void enqueueSyncJob(Integer pageNumber, Integer batchSize, Integer recordLimit, Integer totalProcessed) {
        System.enqueueJob(new SyncAccountsQueueable(pageNumber, batchSize, recordLimit, totalProcessed));
    }

    // Obtém o limite de registros do Custom Setting
    public static Integer getRecordLimitFromCustomSetting() {
        try {
            List<AccountsImportLimit__c> settings = [SELECT AccountsImportLimit__c FROM AccountsImportLimit__c LIMIT 1];
            if (!settings.isEmpty() && settings[0].AccountsImportLimit__c != null) {
                return Integer.valueOf(settings[0].AccountsImportLimit__c);
            } else {
                System.debug('Custom Setting "AccountsImportLimit__c" não configurado ou vazio.');
            }
        } catch (Exception e) {
            System.debug('Erro ao buscar o Custom Setting:' + e.getMessage());
        }
        return 1000; // Valor padrão para evitar erros
    }

    
    public static Boolean getImportaLocaisEntregaFromCustomSetting() {
        try {
            List<AccountsImportLimit__c> settings = [SELECT Importa_Locais_de_Entrega__c FROM AccountsImportLimit__c LIMIT 1];
            if (!settings.isEmpty() && settings[0].Importa_Locais_de_Entrega__c != null) {
                return settings[0].Importa_Locais_de_Entrega__c;
            } else {
                System.debug('Custom Setting "AccountsImportLimit__c" não configurado ou vazio.');
            }
        } catch (Exception e) {
            System.debug('Erro ao buscar o Custom Setting:' + e.getMessage());
        }
        return false;
    }

    // Registra evento no objeto Integration_Event_Log__c para falhas de configuração
    public static void logMissingCustomSetting() {
        try {
            Integration_Event_Log__c log = new Integration_Event_Log__c();
            log.Type_Event__c = 'ImportAccount';
            log.Response_Object__c = 'Favor configurar o custom setting "AccountsImportLimit" com o máximo de contas a ser importada.';
            log.ObjectId__c = '---';
            log.Id = null;
            insert log;
        } catch (Exception e) {
            System.debug('Erro ao registrar log de evento: ' + e.getMessage());
        }
    }

    public class SyncAccountsQueueable implements Queueable, Database.AllowsCallouts {
        private Integer pageNumber;
        private Integer batchSize;
        private Integer recordLimit;
        private Integer totalProcessed;

        public SyncAccountsQueueable(Integer pageNumber, Integer batchSize, Integer recordLimit, Integer totalProcessed) {
            this.pageNumber = pageNumber;
            this.batchSize = batchSize;
            this.recordLimit = recordLimit;
            this.totalProcessed = totalProcessed;
        }

        public void execute(QueueableContext context) {
            List<Integration_Event_Log__c> logs = new List<Integration_Event_Log__c>();
            system.debug('>>>>>> totalProcessed: '+totalProcessed);
            if (totalProcessed >= recordLimit) {
                logs.add(new Integration_Event_Log__c(
                    Type_Event__c = 'ImportAccount',
                    Response_Object__c = 'Limite total de registros atingido: ' + recordLimit,
                    ObjectId__c = null
                ));
                clearLogIds(logs);
                insert logs;
                return;
            }

            ApiResponse response = makeApiCall(pageNumber, batchSize);

            if (response.accounts.isEmpty()) {
                logs.add(new Integration_Event_Log__c(
                    Type_Event__c = 'ImportAccount',
                    Response_Object__c = 'Nenhum registro restante para processar.',
                    ObjectId__c = null
                ));
                clearLogIds(logs);
                insert logs;
                return;
            }

            try {

                List<String> clienteCodigos = new List<String>();
                for (Account acc : response.accounts) {
                    if (acc.Codigo_do_Cliente__c != null) {
                        clienteCodigos.add(acc.Codigo_do_Cliente__c);
                    }
                }

                Map<String, Account> existingAccounts = new Map<String, Account>(
                    [SELECT Codigo_do_Cliente__c FROM Account WHERE Codigo_do_Cliente__c IN :clienteCodigos]
                );

                Integer newAccountsCount = 0;
                Integer updatedAccountsCount = 0;
                List<Account> newAccounts = new List<Account>();
                List<Database.UpsertResult> results = Database.upsert(response.accounts, Account.Codigo_do_Cliente__c, false);
                for (Integer i = 0; i < results.size(); i++) {
                    if (!results[i].isSuccess()) {
                        for (Database.Error error : results[i].getErrors()) {
                            logs.add(new Integration_Event_Log__c(
                                Type_Event__c = 'ImportAccount',
                                Response_Object__c = 'Erro ao inserir/atualizar conta. Codigo_do_Cliente__c: ' +
                                                     response.accounts[i].Codigo_do_Cliente__c + ' | Erro: ' + error.getMessage(),
                                ObjectId__c = null
                            ));
                        }
                    } else {
                        // Incrementa `totalProcessed` apenas para novas contas
                        if (!existingAccounts.containsKey(response.accounts[i].Codigo_do_Cliente__c)) {
                            newAccountsCount++;
                            totalProcessed++;
                            newAccounts.add(response.accounts[i]);
                        } else {
                            updatedAccountsCount++;
                        }
                    }
                }
                
                // Log resumo
                logs.add(new Integration_Event_Log__c(
                    Type_Event__c = 'ImportAccount',
                    Response_Object__c = 'Resumo: ' + newAccountsCount + ' novas contas inseridas, ' + 
                                         updatedAccountsCount + ' contas atualizadas.',
                    ObjectId__c = null
                ));

                Boolean importaLocaisEntrega = getImportaLocaisEntregaFromCustomSetting();
                if (importaLocaisEntrega && !newAccounts.isEmpty()) {
                    createLocalEntregaForAccounts(newAccounts);
                }
            } catch (Exception e) {
                logs.add(new Integration_Event_Log__c(
                    Type_Event__c = 'ImportAccount',
                    Response_Object__c = 'Erro ao inserir/atualizar contas: ' + e.getMessage(),
                    ObjectId__c = null
                ));
            }

            // Perform bulk insert for logs
            if (!logs.isEmpty()) {
                clearLogIds(logs);
                try {
                    insert logs;
                } catch (Exception e) {
                    System.debug('Erro ao inserir logs: ' + e.getMessage());
                }
            }
            

            if (!Test.isRunningTest() && response.hasNext && totalProcessed < recordLimit) {
                enqueueSyncJob(pageNumber + 1, batchSize, recordLimit, totalProcessed);
            } else {
                logs.add(new Integration_Event_Log__c(
                    Type_Event__c = 'ImportAccount',
                    Response_Object__c = 'Sincronização concluída. Total processado: ' + totalProcessed
                ));
                clearLogIds(logs);
                try {
                    insert logs;
                } catch (Exception e) {
                    System.debug('Erro ao registrar log de conclusão: ' + e.getMessage());
                }
            }
        }
    }

    // Faz a chamada à API
    public static ApiResponse makeApiCall(Integer pageNumber, Integer batchSize) {
        List<Account> accountList = new List<Account>();
        Boolean hasNext = false;
    
        HttpRequest req = new HttpRequest();
        req.setMethod('GET');
        req.setEndpoint('callout:Protheus_API/rest/salesforce/listarclientes?page=' + pageNumber + '&pagesize=' + batchSize);
        req.setHeader('Content-Type', 'application/json');
    
        Http http = new Http();
        try {
            HttpResponse res = http.send(req);
            if (res.getStatusCode() == 200) {
                Map<String, Object> responseMap = (Map<String, Object>) JSON.deserializeUntyped(res.getBody());
                Object itemsObj = responseMap.get('items');
                hasNext = (Boolean) responseMap.get('hasNext');
    
                if (itemsObj instanceof List<Object>) {
                    List<Object> items = (List<Object>) itemsObj;
                    for (Object itemObj : items) {
                        if (itemObj instanceof Map<String, Object>) {
                            accountList.add(mapToAccount((Map<String, Object>) itemObj));
                        }
                    }
                }
            } else {
                logIntegrationEvent('ImportAccount', 'Erro na API: ' + res.getStatusCode(), null);
            }
        } catch (Exception e) {
            logIntegrationEvent('ImportAccount', 'Erro na chamada à API: ' + e.getMessage(), null);
        }
    
        return new ApiResponse(accountList, hasNext);
    }

    public static void createLocalEntregaForAccounts(List<Account> newAccounts) {
        List<LocalEntrega__c> localEntregaList = new List<LocalEntrega__c>();
        List<Integration_Event_Log__c> errorLogs = new List<Integration_Event_Log__c>();

        Map<String, String> countryMapping = new Map<String, String>{
			'ANGOLA' => 'AO',
			'ARGENTINA' => 'AR',
			'BENIN' => 'BM',
			'BOLIVIA' => 'BO',
            'BÓSNIA-HERZEGOVINA' => 'BA',
            'BOSNIA-HERZEGOVINA (REPUB' => 'BA',
			'BRASIL' => 'BR',
            'CANADA' => 'CA',
			'CHILE' => 'CL',
            'CHINA' => 'CN',
            'CHINA, REPUBLICA POPULAR' => 'CN',
			'COLOMBIA' => 'CO',
            'CONGO' => 'CG',
            'CONGO, REPUBLICA DEMOCRAT' => 'CG',
            'COREIA' => 'KR',
            'COREIA, REPUBLICA DA' => 'KR',
			'EQUADOR' => 'EC',
			'ESTADOS UNIDOS' => 'US',
			'GANA' => 'GH',
			'HONDURAS' => 'HN',
			'INDIA' => 'IN',
			'JAMAICA' => 'JM',
            'MOCAMBIQUE' => 'MZ',
            'GUATEMALA' => 'GT',
			'PARAGUAI' => 'PY',
			'URUGUAI' => 'UY',
			'PERU' => 'PE',
			'PORTUGAL' => 'PT',
			'SURINAME' => 'SR'
		};

        for (Account acc : newAccounts) {
            LocalEntrega__c localEntrega = new LocalEntrega__c();
            localEntrega.Name = acc.Name;
            localEntrega.Account__c = acc.Id;
            localEntrega.EnderecoMapa__Street__s = acc.BillingStreet;
            localEntrega.EnderecoMapa__City__s = acc.BillingCity;
            localEntrega.EnderecoMapa__PostalCode__s = acc.BillingPostalCode;
            localEntrega.EnderecoMapa__CountryCode__s = countryMapping.containsKey(acc.BillingCountry) 
				? countryMapping.get(acc.BillingCountry) 
				: acc.BillingCountry;
            localEntrega.Ativo__c = true;
            localEntrega.EnderecoPadrao__c = true;
            localEntrega.EnderecoMapa__StateCode__s = acc.BillingState == 'EX' ? null : acc.BillingState;
            localEntregaList.add(localEntrega);
        }
        system.debug('localEntregaList: '+JSON.serialize(localEntregaList));
        if (!localEntregaList.isEmpty()) {
            List<Database.SaveResult> saveResults = Database.insert(localEntregaList, false);
            for (Integer i = 0; i < saveResults.size(); i++) {
                if (!saveResults[i].isSuccess()) {
                    String codigoDoCliente = null;
                    for (Account acc : newAccounts) {
                        if (acc.Id == localEntregaList[i].Account__c) {
                            codigoDoCliente = acc.Codigo_do_Cliente__c;
                            break;
                        }
                    }
                    for (Database.Error error : saveResults[i].getErrors()) {
                        errorLogs.add(new Integration_Event_Log__c(
                            Type_Event__c = 'LocalEntrega',
                            Response_Object__c = 'Erro ao inserir LocalEntrega: ' + error.getMessage() +
                                                 ' | Codigo_do_Cliente: ' + (codigoDoCliente != null ? codigoDoCliente : 'N/A') +
                                                 ' | Account Id: ' + localEntregaList[i].Account__c,
                            ObjectId__c = localEntregaList[i].Account__c
                        ));
                    }
                }
            }
        }
        

        if (!errorLogs.isEmpty()) {
            try {
                insert errorLogs;
            } catch (Exception logEx) {
                System.debug('Erro ao inserir logs de erro: ' + logEx.getMessage());
            }
        }
        
    }

    private static Account mapToAccount(Map<String, Object> item) {
        Account acc = new Account();
        acc.Integracao__c = true;
        acc.Name = (String) item.get('nome_reduzido_do_cliente');
        acc.RazaoSocial__c = (String) item.get('nome_do_cliente');
        acc.LimiteCredito__c = item.containsKey('limitecredito') ? (Decimal) item.get('limitecredito') : null;
        acc.Saldo_Duplicatas__c = item.containsKey('saldoduplicatas') ? (Decimal) item.get('saldoduplicatas') : null;
        acc.BillingStreet = (String) item.get('endereco_do_cliente');
        acc.BillingCity = (String) item.get('municipio_do_cliente');
        acc.BillingState = (String) item.get('estado_do_cliente');
        acc.BillingPostalCode = (String) item.get('cep');
        acc.BillingCountry = (String) item.get('errovariavel'); //API do cliente com erro e sem um tecnico disponivel para arrumar
        acc.CNPJ__c = (String) item.get('cnpj_cpf_do_cliente');
        acc.InscricaoEstadual__c = (String) item.get('inscricao_estadual');
        acc.EmailCobranca__c = (String) item.get('email');
        acc.CodigoVendedor__c = (String) item.get('codigo_do_vendedor');
        acc.Phone = (String) item.get('telefone_do_cliente');
        acc.Codigo_do_Cliente__c = (String) item.get('codigo_do_cliente');
        acc.AccountNumber = (String) item.get('codigo_do_cliente');
        return acc;
    }

    public static void logIntegrationEvent(String type, String response, String objectId) {
        List<Integration_Event_Log__c> logs = new List<Integration_Event_Log__c>();
        Integration_Event_Log__c log = new Integration_Event_Log__c();
        log.Type_Event__c = type;
        log.Response_Object__c = response;
        log.ObjectId__c = objectId != null ? objectId : '---';
        logs.add(log);

        try {
            insert logs;
        } catch (Exception e) {
            System.debug('Erro ao registrar log: ' + e.getMessage());
        }
    }

    private static void clearLogIds(List<Integration_Event_Log__c> logs) {
        for (Integration_Event_Log__c log : logs) {
            log.Id = null;
        }
    }


    public class ApiResponse {
        public List<Account> accounts;
        public Boolean hasNext;a

        public ApiResponse(List<Account> accounts, Boolean hasNext) {
            this.accounts = accounts;
            this.hasNext = hasNext;
        }
    }
}

----------------------------------------------------------------------------------------------------------------------------------------

Database.executeBatch(new UpdateAccountOwnerBatch(), 200);


----------------------------------------------------------------------------------------------------------------------------------------

System.schedule('Atualizar Owners de Accounts', '0 0 1 * * ?', new UpdateAccountOwnerBatch());


----------------------------------------------------------------------------------------------------------------------------------------

