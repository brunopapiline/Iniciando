public class AccountTriggerHandler {
    public static void processAccountsOwnerUpdate(List<Account> accounts) {
        List<Account> accountsToProcess = new List<Account>();

        Map<Id, Account> oldAccounts = new Map<Id, Account>();
        if (Trigger.isUpdate) {
            oldAccounts = (Map<Id, Account>) Trigger.oldMap; 
        }

        for (Account acc : accounts) {
            Account oldAcc = oldAccounts.containsKey(acc.Id) ? oldAccounts.get(acc.Id) : null;

            if (Trigger.isInsert && acc.CodigoVendedor__c != null) {
                accountsToProcess.add(acc);
            } else if (Trigger.isUpdate && oldAcc != null && acc.CodigoVendedor__c != oldAcc.CodigoVendedor__c) {
                accountsToProcess.add(acc);
            }
        }

        if (!accountsToProcess.isEmpty()) {
            Set<String> codigoVendedorSet = new Set<String>();
            for (Account acc : accountsToProcess) {
                if (acc.CodigoVendedor__c != null) {
                    codigoVendedorSet.add(acc.CodigoVendedor__c);
                }
            }

            if (!codigoVendedorSet.isEmpty()) {
                Database.executeBatch(new AccountBatchProcessor(codigoVendedorSet));
            }
        }
    }
}