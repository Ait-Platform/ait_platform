with open('templates/program_billing/ai_onboarding.html', 'r', encoding='utf-8') as f:
    content = f.read()

target_init = '''        try {
          const state = JSON.parse(saved);
          this.currentBillIndex = state.currentBillIndex || 1;
          this.view = state.view || 'upload';
          if(state.masterPropertyDetails) this.masterPropertyDetails = state.masterPropertyDetails;
          if(state.statementProfiles && state.statementProfiles.length > 0) this.statementProfiles = state.statementProfiles;
          if(state.allBills) this.allBills = state.allBills;
          if(state.allMeters) this.allMeters = state.allMeters;'''

injection_init = '''        try {
          const state = JSON.parse(saved);
          this.currentBillIndex = state.currentBillIndex || 1;
          this.view = state.view || 'upload';
          
          if(state.totalBills) this.totalBills = state.totalBills;
          if(state.statements) this.statements = state.statements;
          if(state.isBulk) this.isBulk = state.isBulk;
          if(state.subMetersExpected !== undefined) this.subMetersExpected = state.subMetersExpected;
          if(state.totalProperties) this.totalProperties = state.totalProperties;
          
          if(state.masterPropertyDetails) this.masterPropertyDetails = state.masterPropertyDetails;
          if(state.statementProfiles && state.statementProfiles.length > 0) this.statementProfiles = state.statementProfiles;
          if(state.allBills) this.allBills = state.allBills;
          if(state.allMeters) this.allMeters = state.allMeters;'''

content = content.replace(target_init, injection_init)


target_save = '''    saveState() {
      const state = {
        currentBillIndex: this.currentBillIndex,
        view: this.view,
        masterPropertyDetails: this.masterPropertyDetails,
        statementProfiles: this.statementProfiles,
        allBills: this.allBills,
        allMeters: this.allMeters
      };'''

injection_save = '''    saveState() {
      const state = {
        totalBills: this.totalBills,
        statements: this.statements,
        isBulk: this.isBulk,
        subMetersExpected: this.subMetersExpected,
        totalProperties: this.totalProperties,
        
        currentBillIndex: this.currentBillIndex,
        view: this.view,
        masterPropertyDetails: this.masterPropertyDetails,
        statementProfiles: this.statementProfiles,
        allBills: this.allBills,
        allMeters: this.allMeters
      };'''

content = content.replace(target_save, injection_save)

with open('templates/program_billing/ai_onboarding.html', 'w', encoding='utf-8') as f:
    f.write(content)
